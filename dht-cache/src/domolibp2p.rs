// Gossip includes
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    // Gossipsub, GossipsubEvent, GossipsubMessage,
    IdentTopic as Topic,
    MessageAuthenticity,
    ValidationMode,
};
use libp2p::{gossipsub, tcp};

use rsa::pkcs8::EncodePrivateKey;
use rsa::RsaPrivateKey;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
//

use libp2p::core::{muxing::StreamMuxerBox, transport, transport::upgrade::Version};

use libp2p::noise;
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::yamux;
//use libp2p::tcp::TcpConfig;
use libp2p::Transport;

use libp2p::{identity, mdns, swarm::NetworkBehaviour, PeerId, Swarm};

use libp2p::swarm::SwarmBuilder;
use std::time::Duration;

use crate::Error;

const KEY_SIZE: usize = 32;

pub fn parse_hex_key(s: &str) -> Result<PreSharedKey, Error> {
    if s.len() == KEY_SIZE * 2 {
        let mut r = [0u8; KEY_SIZE];
        for i in 0..KEY_SIZE {
            let ret = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16);
            match ret {
                Ok(res) => {
                    r[i] = res;
                }
                Err(_e) => return Err(Error::Hex("Error while parsing".into())),
            }
        }
        let psk = PreSharedKey::new(r);

        Ok(psk)
    } else {
        Err(Error::Hex(format!(
            "Len Error: expected {} but got {}",
            KEY_SIZE * 2,
            s.len()
        )))
    }
}

pub fn build_transport(
    key_pair: identity::Keypair,
    psk: PreSharedKey,
) -> transport::Boxed<(PeerId, StreamMuxerBox)> {
    let noise_config = noise::Config::new(&key_pair).unwrap();
    let yamux_config = yamux::Config::default();

    let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));

    base_transport
        .and_then(move |socket, _| PnetConfig::new(psk).handshake(socket))
        .upgrade(Version::V1)
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}

pub fn generate_rsa_key() -> (Vec<u8>, Vec<u8>) {
    let mut rng = rand::thread_rng();
    let bits = 2048;
    let private_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
    let pem = private_key
        .to_pkcs8_pem(Default::default())
        .unwrap()
        .as_bytes()
        .to_vec();
    let der = private_key.to_pkcs8_der().unwrap().as_bytes().to_vec();
    (pem, der)
}

pub async fn start(
    shared_key: PreSharedKey,
    local_key_pair: identity::Keypair,
    loopback_only: bool,
) -> Result<Swarm<DomoBehaviour>, Error> {
    let local_peer_id = PeerId::from(local_key_pair.public());

    let transport = build_transport(local_key_pair.clone(), shared_key);

    // Create a swarm to manage peers and events.
    let mut swarm = {
        let behaviour = DomoBehaviour::new(&local_key_pair)?;

        SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build()
    };

    if !loopback_only {
        // Listen on all interfaces and whatever port the OS assigns.
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    } else {
        // Listen only on loopack interface
        swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;
    }

    Ok(swarm)
}

// We create a custom network behaviour that combines mDNS and gossipsub.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "OutEvent")]
pub struct DomoBehaviour {
    pub mdns: libp2p::mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
}

impl DomoBehaviour {
    pub fn new(local_key_pair: &crate::Keypair) -> Result<Self, Error> {
        let local_peer_id = PeerId::from(local_key_pair.public());
        let topic_persistent_data = Topic::new("domo-persistent-data");
        let topic_volatile_data = Topic::new("domo-volatile-data");
        let topic_config = Topic::new("domo-config");

        let mdnsconf = mdns::Config {
            ttl: Duration::from_secs(600),
            query_interval: Duration::from_secs(30),
            enable_ipv6: false,
        };

        let mdns = mdns::tokio::Behaviour::new(mdnsconf, local_peer_id)?;

        // To content-address message, we can take the hash of message and use it as an ID.
        let message_id_fn = |message: &gossipsub::Message| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            MessageId::from(s.finish().to_string())
        };

        // Set a custom gossipsub
        // SAFETY: hard-coded configuration
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .idle_timeout(Duration::from_secs(10))
            .heartbeat_interval(Duration::from_secs(3)) // This is set to aid debugging by not cluttering the log space
            .check_explicit_peers_ticks(10)
            .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
            .message_id_fn(message_id_fn) // content-address messages. No two messages of the
            // same content will be propagated.
            .build()
            .expect("Valid config");

        // build a gossipsub network behaviour
        let mut gossipsub = gossipsub::Behaviour::new(
            MessageAuthenticity::Signed(local_key_pair.to_owned()),
            gossipsub_config,
        )
        .expect("Correct configuration");

        // subscribes to persistent data topic
        gossipsub.subscribe(&topic_persistent_data).unwrap();

        // subscribes to volatile data topic
        gossipsub.subscribe(&topic_volatile_data).unwrap();

        // subscribes to config topic
        gossipsub.subscribe(&topic_config).unwrap();

        let behaviour = DomoBehaviour { mdns, gossipsub };

        Ok(behaviour)
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum OutEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<mdns::Event> for OutEvent {
    fn from(v: mdns::Event) -> Self {
        Self::Mdns(v)
    }
}

impl From<gossipsub::Event> for OutEvent {
    fn from(v: gossipsub::Event) -> Self {
        Self::Gossipsub(v)
    }
}
