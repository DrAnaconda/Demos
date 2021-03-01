using System;
using System.Collections.Generic;
using System.Text;

namespace Demos
{
    public static class NewsAggregationBuilder
    {
        private static FilterDefinition<NewsModel> getPublicFilter()
        {
            return Builders<NewsModel>.Filter.And(
                Builders<NewsModel>.Filter.Eq(x => x.scope, NewsScopes.Global),
                Builders<NewsModel>.Filter.Lte(x => x.postedOrChanged, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds()));
        }
        private static FilterDefinition<NewsModel> getPrivateFilter(string[] requiredParents)
        {
            return Builders<NewsModel>.Filter.And(
                Builders<NewsModel>.Filter.In(x => x.parent, requiredParents),
                Builders<NewsModel>.Filter.Eq(x => x.scope, NewsScopes.Building),
                Builders<NewsModel>.Filter.Lte(x => x.postedOrChanged, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds()));
        }
        private static SortDefinition<NewsModel> getDefaultSort()
        {
            return Builders<NewsModel>.Sort.Descending(x => x.postedOrChanged);
        }
        private static BsonDocument addIsLikedByField(string requesterId)
        {
            var input = GE.PropertyName<NewsModel>(x => x.likedBy, false);
            var isLikedField = GE.PropertyName<NewsListProjection>(x => x.isLiked, false);
            return new BsonDocument("$addFields", MongoHelper.addFieldAnyElementInTemplate(input,
                string.Empty, requesterId, isLikedField));
        }
        public static Task<List<NewsListProjection>> buildPublicNewsList(
            this IMongoCollection<NewsModel> collection,
            string requesterId, int page = 0, int pageSize = 10)
        {
            var filter = getPublicFilter(); var sort = getDefaultSort();
            var query = collection.WithReadPreference(ReadPreference.SecondaryPreferred)
                .Aggregate().Match(filter).Sort(sort)
                .Skip(pageSize * page).Limit(pageSize)
                .AppendStage<NewsListProjection>(addIsLikedByField(requesterId))
                .Project<NewsListProjection>(MongoHelper.IQProjectionBuilder<NewsListProjection>());
            return query.ToListAsync();
        }

        public static Task<List<NewsListProjection>> buildPrivateNewsList(
            this IMongoCollection<NewsModel> collection,
            string requesterId, string[] requiredBuildings, int page = 0, int pageSize = 10)
        {
            var filter = getPrivateFilter(requiredBuildings); var sort = getDefaultSort();
            var query = collection.WithReadPreference(ReadPreference.SecondaryPreferred).Aggregate()
                .Match(filter).Sort(sort)
                .Skip(pageSize * page).Limit(pageSize)
                .AppendStage<NewsListProjection>(addIsLikedByField(requesterId))
                .Project<NewsListProjection>(MongoHelper.IQProjectionBuilder<NewsListProjection>());
            return query.ToListAsync();
        }

        public static Task<NewsListProjection> extractCertainNews(
            this IMongoCollection<NewsModel> collection, string newsId,
            string requesterId)
        {
            var filter = Builders<NewsModel>.Filter.Eq(x => x._id, newsId);
            var query = collection.WithReadPreference(ReadPreference.SecondaryPreferred).Aggregate().Match(filter)
                .AppendStage<NewsListProjection>(addIsLikedByField(requesterId))
                .Project<NewsListProjection>(MongoHelper.IQProjectionBuilder<NewsListProjection>());
            return query.FirstOrDefaultAsync();
        }
    }
}
