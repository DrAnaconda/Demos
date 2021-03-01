using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MongoDBDemos
{
    public interface IMongoRepositoryCollection
    {
        public IMongoCollection<T> getCollection<T>();
    }
    internal interface IMongoDefaultRequests : IMongoRepositoryCollection
    {
        Task<IClientSessionHandle> initTransactionAsync(ClientSessionOptions options = null);

        Task upsertBatchAsync<T>(IEnumerable<T> upsertObjects, IClientSessionHandle? session) where T : IMongoEntity;

        Task<List<T>> getRandomObjectsAsync<T>(int size);
        Task<Result> getSingleObjectByIdWithProjectionAsync<Result>(string id);
        Task<List<Result>> getMultipleObjectsByIdWithProjectionAsync<Result>(string[] ids) where Result : IMongoEntity;
        Task mergeDocumentAsync<OriginalObjectType>(string documentId, OriginalObjectType patchDocument, IClientSessionHandle? session);
        Task mergeDocumentsBatchAsync<OriginalObjectType>(OriginalObjectType[] patchDocument, IClientSessionHandle? session) where OriginalObjectType : IMongoEntity;
        Task mergeDocumentsBatchAsync<OriginalObjectType>(string[] ids, OriginalObjectType[] patchDocument, IClientSessionHandle? session);
    }
    public abstract class BaseMongoRepo : IMongoDefaultRequests
    {
        protected readonly MongoService mService;

        protected BaseMongoRepo(MongoService mService)
        {
            this.mService = mService;
        }

        abstract public IMongoCollection<T> getCollection<T>();

        public Task<IClientSessionHandle> initTransactionAsync(ClientSessionOptions options = null)
        {
            return mService.client.StartSessionAsync(options);
        }

        public Task<List<T>> getRandomObjectsAsync<T>(int size)
        {
            return getCollection<T>().AsQueryable().Sample(size).ToListAsync();
        }

        public Task<List<Result>> getMultipleObjectsByIdWithProjectionAsync<Result>(string[] ids) where Result : IMongoEntity
        {
            var filter = Builders<Result>.Filter.In(x => x._id, ids);
            return buildProjection(filter).ToListAsync();
        }

        public Task mergeDocumentAsync<OriginalObjectType>(string documentId, OriginalObjectType patchDocument, IClientSessionHandle? session)
        {
            var filter = Builders<OriginalObjectType>.Filter.Eq(GE.PropertyName<MongoEntity>(x => x._id, false), new ObjectId(documentId));
            var update = new BsonDocument() { { "$set", patchDocument.ToBsonDocument() } };
            return session == null ?
                getCollection<OriginalObjectType>().UpdateOneAsync(filter, update)
                :
                getCollection<OriginalObjectType>().UpdateOneAsync(session, filter, update);
        }

        public Task mergeDocumentsBatchAsync<OriginalObjectType>(OriginalObjectType[] patchDocument, IClientSessionHandle? session) where OriginalObjectType : IMongoEntity
        {
            string[] ids = patchDocument.Where(x => x._id != null).Distinct().Select(x => x._id).ToArray();
            return mergeDocumentsBatchAsync(ids, patchDocument, session);
        }

        public Task mergeDocumentsBatchAsync<OriginalObjectType>(string[] ids, OriginalObjectType[] patchDocument, IClientSessionHandle? session)
        {
            if (ids.Length != patchDocument.Length)
                throw new InvalidOperationException($"{nameof(ids)} collection and {nameof(patchDocument)} have different size");
            var bulkOps = new List<WriteModel<OriginalObjectType>>();
            var idName = GE.PropertyName<MongoEntity>(x => x._id, false);
            for (int i = 0; i < ids.Length; i++)
            {
                var update = new BsonDocument() { { "$set", patchDocument[i].ToBsonDocument() } };
                bulkOps.Add(new UpdateOneModel<OriginalObjectType>(
                    Builders<OriginalObjectType>.Filter.Eq(idName, new ObjectId(ids[i])), update) { IsUpsert = false });
            }
            return session == null
                ? getCollection<OriginalObjectType>().BulkWriteAsync(bulkOps)
                : getCollection<OriginalObjectType>().BulkWriteAsync(session, bulkOps);
        }

        private IAggregateFluent<Result> buildProjection<Result>(FilterDefinition<Result> filter)
        {
            var projection = MongoHelper.IQProjectionBuilder<Result>();
            var projectionStage = new BsonDocument("$project", projection);
            return getCollection<Result>().Aggregate().Match(filter).AppendStage<Result>(projectionStage);
        }
    }

    internal interface ITicketActions
    {
        public Task closeTicketAsync(string ticketId, IClientSessionHandle transaction);
        public Task rejectTicketAsync(string ticketId, string? rejectionComment, IClientSessionHandle transaction);
        public Task assignUserToTicketAsync(string ticketId, string assigneeId, IClientSessionHandle transaction);
    }
    internal interface ITicketRepository : ITicketActions
    {
        public Task<List<TicketAggregatedUserView>> paginateRequestsForAssetsAsync(string[] assetsIds, int page = 0, int pageSize = 10);
        public Task<TicketAggregatedUserView> getCertainTicketAsync(string ticketId);
    }
    public class TicketsRepository : BaseMongoRepo, ITicketRepository
    {
        public const string collectionName = "tickets";

        private readonly UserTicketViewBuilder ticketsViewBuilder = new UserTicketViewBuilder();

        public TicketsRepository(MongoService mService) : base(mService)
        {
            ensureIndexes();
        }

        private void ensureIndexes()
        {
            var createRequestList = new List<CreateIndexModel<TicketModel>>
            {
                new CreateIndexModel<TicketModel>(
                new IndexKeysDefinitionBuilder<TicketModel>()
                    .Descending(x => x.creationTime).Descending(x => x.closedTime).Descending(x => x.acceptedTime),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<TicketModel>(
                    new IndexKeysDefinitionBuilder<TicketModel>().Descending(x => x.authorId),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<TicketModel>(
                    new IndexKeysDefinitionBuilder<TicketModel>().Descending(x => x.positionId),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<TicketModel>(
                    new IndexKeysDefinitionBuilder<TicketModel>().Descending(x => x.apartId),
                new CreateIndexOptions() { Unique = false }),
                new CreateIndexModel<TicketModel>(
                    new IndexKeysDefinitionBuilder<TicketModel>().Descending(x => x.executerId),
                new CreateIndexOptions() { Unique = false })
            };

            getCollection<TicketModel>().Indexes.CreateManyAsync(createRequestList);
        }

        public override IMongoCollection<T> getCollection<T>()
        {
            return mService.getMainDatabase.GetCollection<T>(collectionName);
        }

        public Task<List<TicketAggregatedUserView>> paginateRequestsForAssetsAsync(string[] assetsIds, int page = 0, int pageSize = 10)
        {
            return ticketsViewBuilder.buildTicketsForAssets(assetsIds, this.getCollection<TicketAggregatedUserView>(), page, pageSize);
        }

        public Task<TicketAggregatedUserView> getCertainTicketAsync(string ticketId)
        {
            return ticketsViewBuilder.getCertainTicket(ticketId, getCollection<TicketAggregatedUserView>());
        }

        public Task closeTicketAsync(string ticketId, IClientSessionHandle transaction)
        {
            var filter = Builders<TicketModel>.Filter.And(
                Builders<TicketModel>.Filter.Eq(x => x._id, ticketId),
                Builders<TicketModel>.Filter.Eq(x => x.status, TicketModel.TicketStatus.Assigned));

            var update = Builders<TicketModel>.Update.Set(x => x.status, TicketModel.TicketStatus.Closed)
                .Set(x => x.closedTime, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds());

            return (transaction != null) ?
                getCollection<TicketModel>().UpdateOneAsync(transaction, filter, update)
                :
                getCollection<TicketModel>().UpdateOneAsync(filter, update);
        }

        public Task rejectTicketAsync(string ticketId, string? rejectionComment, IClientSessionHandle transaction)
        {
            var filter = Builders<TicketModel>.Filter.And(
                Builders<TicketModel>.Filter.Eq(x => x._id, ticketId),
                Builders<TicketModel>.Filter.Lte(x => x.status, TicketModel.TicketStatus.Assigned));

            var update = Builders<TicketModel>.Update.Set(x => x.status, TicketModel.TicketStatus.Rejected)
                .Set(x => x.rejectionTime, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
                .Set(x => x.rejectionComment, rejectionComment);

            return (transaction != null) ?
                getCollection<TicketModel>().UpdateOneAsync(transaction, filter, update)
                :
                getCollection<TicketModel>().UpdateOneAsync(filter, update);
        }

        public Task assignUserToTicketAsync(string ticketId, string assigneeId, IClientSessionHandle transaction)
        {
            var filter = Builders<TicketModel>.Filter.And(
                Builders<TicketModel>.Filter.Eq(x => x._id, ticketId),
                Builders<TicketModel>.Filter.Ne(x => x.status, TicketModel.TicketStatus.Rejected),
                Builders<TicketModel>.Filter.Lte(x => x.status, TicketModel.TicketStatus.Closed));

            var update = Builders<TicketModel>.Update.Set(x => x.status, TicketModel.TicketStatus.Assigned)
                .Set(x => x.acceptedTime, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds())
                .Set(x => x.executerId, assigneeId);

            return (transaction != null) ?
                getCollection<TicketModel>().UpdateOneAsync(transaction, filter, update)
                :
                getCollection<TicketModel>().UpdateOneAsync(filter, update);
        }
    }
}
