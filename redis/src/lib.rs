use bb8_redis::{
    bb8::{Pool, RunError},
    redis::{AsyncCommands, IntoConnectionInfo, RedisError as OriginalRedisError},
    RedisConnectionManager, RedisPool,
};
use rarity_cache::{
    entity::{
        channel::{
            attachment::{AttachmentEntity, AttachmentRepository},
            category_channel::{CategoryChannelEntity, CategoryChannelRepository},
            group::{GroupEntity, GroupRepository},
            message::{MessageEntity, MessageRepository},
            private_channel::{PrivateChannelEntity, PrivateChannelRepository},
            text_channel::{TextChannelEntity, TextChannelRepository},
            voice_channel::{VoiceChannelEntity, VoiceChannelRepository},
        },
        gateway::presence::{PresenceEntity, PresenceRepository},
        guild::{
            emoji::{EmojiEntity, EmojiRepository},
            member::{MemberEntity, MemberRepository},
            role::{RoleEntity, RoleRepository},
            GuildEntity, GuildRepository,
        },
        user::{UserEntity, UserRepository},
        voice::{VoiceStateEntity, VoiceStateRepository},
        Entity,
    },
    repository::{GetEntityFuture, ListEntitiesFuture, RemoveEntityFuture, UpsertEntityFuture},
    Backend, Cache, Repository,
};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use twilight_model::id::{AttachmentId, ChannelId, EmojiId, GuildId, MessageId, RoleId, UserId};

pub type RedisCache = Cache<RedisBackend>;
pub type RedisError = RunError<OriginalRedisError>;

pub trait RedisEntity: Entity {
    fn key(id: Self::Id) -> Vec<u8>;
}

impl RedisEntity for AttachmentEntity {
    fn key(id: AttachmentId) -> Vec<u8> {
        format!("at:{}", id).into_bytes()
    }
}

impl RedisEntity for CategoryChannelEntity {
    fn key(id: ChannelId) -> Vec<u8> {
        format!("cc:{}", id).into_bytes()
    }
}

impl RedisEntity for EmojiEntity {
    fn key(id: EmojiId) -> Vec<u8> {
        format!("em:{}", id).into_bytes()
    }
}

impl RedisEntity for GroupEntity {
    fn key(id: ChannelId) -> Vec<u8> {
        format!("gr:{}", id).into_bytes()
    }
}

impl RedisEntity for GuildEntity {
    fn key(id: GuildId) -> Vec<u8> {
        format!("g:{}", id).into_bytes()
    }
}

impl RedisEntity for MemberEntity {
    fn key((guild_id, user_id): (GuildId, UserId)) -> Vec<u8> {
        format!("m:{}:{}", guild_id, user_id).into_bytes()
    }
}

impl RedisEntity for MessageEntity {
    fn key(id: MessageId) -> Vec<u8> {
        format!("ms:{}", id).into_bytes()
    }
}

impl RedisEntity for PresenceEntity {
    fn key((guild_id, user_id): (GuildId, UserId)) -> Vec<u8> {
        format!("pr:{}:{}", guild_id, user_id).into_bytes()
    }
}

impl RedisEntity for PrivateChannelEntity {
    fn key(id: ChannelId) -> Vec<u8> {
        format!("cp:{}", id).into_bytes()
    }
}

impl RedisEntity for RoleEntity {
    fn key(id: RoleId) -> Vec<u8> {
        format!("r:{}", id).into_bytes()
    }
}

impl RedisEntity for TextChannelEntity {
    fn key(id: ChannelId) -> Vec<u8> {
        format!("ct:{}", id).into_bytes()
    }
}

impl RedisEntity for UserEntity {
    fn key(id: UserId) -> Vec<u8> {
        format!("u:{}", id).into_bytes()
    }
}

impl RedisEntity for VoiceChannelEntity {
    fn key(id: ChannelId) -> Vec<u8> {
        format!("cv:{}", id).into_bytes()
    }
}

impl RedisEntity for VoiceStateEntity {
    fn key((guild_id, user_id): (GuildId, UserId)) -> Vec<u8> {
        format!("v:{}:{}", guild_id, user_id).into_bytes()
    }
}

pub struct RedisRepository<T>(RedisBackend, PhantomData<T>);

impl<T> RedisRepository<T> {
    fn new(backend: RedisBackend) -> Self {
        Self(backend, PhantomData)
    }
}

impl<T: DeserializeOwned + Serialize + RedisEntity + Sync> Repository<T, RedisBackend>
    for RedisRepository<T>
{
    fn backend(&self) -> RedisBackend {
        self.0.clone()
    }

    fn get(&self, entity_id: T::Id) -> GetEntityFuture<'_, T, RedisError> {
        Box::pin(async move {
            let mut conn = (self.0).0.get().await?;
            let conn = conn.as_mut().unwrap();
            let bytes: Vec<u8> = conn.get(T::key(entity_id)).await?;

            Ok(Some(serde_cbor::from_slice::<T>(dbg!(&bytes)).unwrap()))
        })
    }

    fn list(&self) -> ListEntitiesFuture<'_, T, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn remove(&self, entity_id: T::Id) -> RemoveEntityFuture<'_, RedisError> {
        Box::pin(async move {
            let mut conn = (self.0).0.get().await?;
            let conn = conn.as_mut().unwrap();
            conn.del(T::key(entity_id)).await?;

            Ok(())
        })
    }

    fn upsert(&self, entity: T) -> UpsertEntityFuture<'_, RedisError> {
        Box::pin(async move {
            let bytes = serde_cbor::to_vec(&entity).unwrap();
            let mut conn = (self.0).0.get().await?;
            let conn = conn.as_mut().unwrap();
            conn.set(T::key(entity.id()), bytes).await?;

            Ok(())
        })
    }
}

impl AttachmentRepository<RedisBackend> for RedisRepository<AttachmentEntity> {}

impl CategoryChannelRepository<RedisBackend> for RedisRepository<CategoryChannelEntity> {}

impl EmojiRepository<RedisBackend> for RedisRepository<EmojiEntity> {}

impl GroupRepository<RedisBackend> for RedisRepository<GroupEntity> {}

impl GuildRepository<RedisBackend> for RedisRepository<GuildEntity> {
    fn channel_ids(
        &self,
        _: GuildId,
    ) -> rarity_cache::repository::ListEntityIdsFuture<'_, ChannelId, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn channels(
        &self,
        _: GuildId,
    ) -> ListEntitiesFuture<'_, rarity_cache::entity::channel::GuildChannelEntity, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn emoji_ids(
        &self,
        _: GuildId,
    ) -> rarity_cache::repository::ListEntityIdsFuture<'_, EmojiId, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn member_ids(
        &self,
        _: GuildId,
    ) -> rarity_cache::repository::ListEntityIdsFuture<'_, UserId, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn members(&self, _: GuildId) -> ListEntitiesFuture<'_, MemberEntity, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn presence_ids(
        &self,
        _: GuildId,
    ) -> rarity_cache::repository::ListEntityIdsFuture<'_, UserId, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn presences(&self, _: GuildId) -> ListEntitiesFuture<'_, PresenceEntity, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn role_ids(
        &self,
        _: GuildId,
    ) -> rarity_cache::repository::ListEntityIdsFuture<'_, RoleId, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn voice_state_ids(
        &self,
        _: GuildId,
    ) -> rarity_cache::repository::ListEntityIdsFuture<'_, UserId, RedisError> {
        unimplemented!("not implemented by this backend");
    }

    fn voice_states(&self, _: GuildId) -> ListEntitiesFuture<'_, VoiceStateEntity, RedisError> {
        unimplemented!("not implemented by this backend");
    }
}

impl MemberRepository<RedisBackend> for RedisRepository<MemberEntity> {}

impl MessageRepository<RedisBackend> for RedisRepository<MessageEntity> {}

impl PresenceRepository<RedisBackend> for RedisRepository<PresenceEntity> {}

impl PrivateChannelRepository<RedisBackend> for RedisRepository<PrivateChannelEntity> {}

impl RoleRepository<RedisBackend> for RedisRepository<RoleEntity> {}

impl TextChannelRepository<RedisBackend> for RedisRepository<TextChannelEntity> {}

impl VoiceChannelRepository<RedisBackend> for RedisRepository<VoiceChannelEntity> {}

impl VoiceStateRepository<RedisBackend> for RedisRepository<VoiceStateEntity> {}

impl UserRepository<RedisBackend> for RedisRepository<UserEntity> {
    fn guild_ids(
        &self,
        _: UserId,
    ) -> rarity_cache::repository::ListEntityIdsFuture<'_, GuildId, RedisError> {
        unimplemented!("not implemented by this backend")
    }
}

/// `rarity-cache` backend for the [Redis] database.
///
/// [Redis]: https://docs.rs/redis
#[derive(Clone)]
pub struct RedisBackend(RedisPool);

impl RedisBackend {
    /// Create a new `rarity-cache` Redis backend with a provided instance.
    pub fn new(redis: RedisPool) -> Self {
        Self(redis)
    }

    /// Shortcut for `RedisPool::new` and [`new`].
    ///
    /// [`new`]: #method.new
    pub async fn from_uri<T: IntoConnectionInfo>(uri: T) -> Self {
        let manager = RedisConnectionManager::new(uri).unwrap();
        let pool = RedisPool::new(Pool::builder().build(manager).await.unwrap());
        Self(pool)
    }

    fn repo<T>(&self) -> RedisRepository<T> {
        RedisRepository::new(self.clone())
    }
}

impl Backend for RedisBackend {
    type Error = RedisError;
    type AttachmentRepository = RedisRepository<AttachmentEntity>;
    type CategoryChannelRepository = RedisRepository<CategoryChannelEntity>;
    type EmojiRepository = RedisRepository<EmojiEntity>;
    type GroupRepository = RedisRepository<GroupEntity>;
    type GuildRepository = RedisRepository<GuildEntity>;
    type MemberRepository = RedisRepository<MemberEntity>;
    type MessageRepository = RedisRepository<MessageEntity>;
    type PresenceRepository = RedisRepository<PresenceEntity>;
    type PrivateChannelRepository = RedisRepository<PrivateChannelEntity>;
    type RoleRepository = RedisRepository<RoleEntity>;
    type TextChannelRepository = RedisRepository<TextChannelEntity>;
    type UserRepository = RedisRepository<UserEntity>;
    type VoiceChannelRepository = RedisRepository<VoiceChannelEntity>;
    type VoiceStateRepository = RedisRepository<VoiceStateEntity>;

    fn attachments(&self) -> Self::AttachmentRepository {
        self.repo()
    }

    fn category_channels(&self) -> Self::CategoryChannelRepository {
        self.repo()
    }

    fn emojis(&self) -> Self::EmojiRepository {
        self.repo()
    }

    fn groups(&self) -> Self::GroupRepository {
        self.repo()
    }

    fn guilds(&self) -> Self::GuildRepository {
        self.repo()
    }

    fn members(&self) -> Self::MemberRepository {
        self.repo()
    }

    fn messages(&self) -> Self::MessageRepository {
        self.repo()
    }

    fn presences(&self) -> Self::PresenceRepository {
        self.repo()
    }

    fn private_channels(&self) -> Self::PrivateChannelRepository {
        self.repo()
    }

    fn roles(&self) -> Self::RoleRepository {
        self.repo()
    }

    fn text_channels(&self) -> Self::TextChannelRepository {
        self.repo()
    }

    fn users(&self) -> Self::UserRepository {
        self.repo()
    }

    fn voice_channels(&self) -> Self::VoiceChannelRepository {
        self.repo()
    }

    fn voice_states(&self) -> Self::VoiceStateRepository {
        self.repo()
    }
}
