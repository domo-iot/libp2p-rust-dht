// Gossip includes
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity,
    ValidationMode,
};
use libp2p::{gossipsub, tcp};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
//

use libp2p::core::{
    either::EitherTransport, muxing::StreamMuxerBox, transport, transport::upgrade::Version,
};

use libp2p::noise;
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::yamux::YamuxConfig;
//use libp2p::tcp::TcpConfig;
use libp2p::Transport;

use libp2p::{identity, mdns, swarm::NetworkBehaviour, PeerId, Swarm};

use libp2p::swarm::SwarmBuilder;
use std::error::Error;
use std::time::Duration;

pub(crate) const KEY_SIZE: usize = 32;

pub fn build_transport(
    key_pair: identity::Keypair,
    psk: Option<PreSharedKey>,
) -> transport::Boxed<(PeerId, StreamMuxerBox)> {
    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&key_pair)
        .unwrap();
    let noise_config = noise::NoiseConfig::xx(noise_keys).into_authenticated();
    let yamux_config = YamuxConfig::default();

    let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
    let maybe_encrypted = match psk {
        Some(psk) => EitherTransport::Left(
            base_transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
        ),
        None => EitherTransport::Right(base_transport),
    };
    maybe_encrypted
        .upgrade(Version::V1)
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}

pub async fn start(
    shared_key: [u8; KEY_SIZE],
    local_key_pair: identity::Keypair,
    loopback_only: bool,
) -> Result<Swarm<DomoBehaviour>, Box<dyn Error>> {
    let local_peer_id = PeerId::from(local_key_pair.public());

    // Create a Gossipsub topic
    let topic_node_manager = Topic::new("domo-node-manager");
    let topic_persistent_data = Topic::new("domo-persistent-data");
    let topic_volatile_data = Topic::new("domo-volatile-data");
    let topic_config = Topic::new("domo-config");

    let psk = Some(PreSharedKey::new(shared_key));

    let transport = build_transport(local_key_pair.clone(), psk);

    // Create a swarm to manage peers and events.
    let mut swarm = {
        let mdnsconf = mdns::Config {
            ttl: Duration::from_secs(600),
            query_interval: Duration::from_secs(580),
            enable_ipv6: false,
        };

        let mdns = mdns::tokio::Behaviour::new(mdnsconf)?;

        // To content-address message, we can take the hash of message and use it as an ID.
        let message_id_fn = |message: &GossipsubMessage| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            MessageId::from(s.finish().to_string())
        };

        // Set a custom gossipsub
        let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
            .idle_timeout(Duration::from_secs(60 * 60 * 24))
            .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
            .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
            .message_id_fn(message_id_fn) // content-address messages. No two messages of the
            // same content will be propagated.
            .build()
            .expect("Valid config");

        // build a gossipsub network behaviour
        let mut gossipsub: gossipsub::Gossipsub = gossipsub::Gossipsub::new(
            MessageAuthenticity::Signed(local_key_pair),
            gossipsub_config,
        )
        .expect("Correct configuration");

        // subscribes to node manager topic
        gossipsub.subscribe(&topic_node_manager).unwrap();

        // subscribes to persistent data topic
        gossipsub.subscribe(&topic_persistent_data).unwrap();

        // subscribes to volatile data topic
        gossipsub.subscribe(&topic_volatile_data).unwrap();

        // subscribes to config topic
        gossipsub.subscribe(&topic_config).unwrap();

        let behaviour = DomoBehaviour { mdns, gossipsub };
        //Swarm::new(transport, behaviour, local_peer_id)

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
#[behaviour(out_event = "OutEvent")]
pub struct DomoBehaviour {
    pub mdns: libp2p::mdns::tokio::Behaviour,
    pub gossipsub: Gossipsub,
}

#[derive(Debug)]
pub enum OutEvent {
    Gossipsub(GossipsubEvent),
    Mdns(mdns::Event),
}

impl From<mdns::Event> for OutEvent {
    fn from(v: mdns::Event) -> Self {
        Self::Mdns(v)
    }
}

impl From<GossipsubEvent> for OutEvent {
    fn from(v: GossipsubEvent) -> Self {
        Self::Gossipsub(v)
    }
}
