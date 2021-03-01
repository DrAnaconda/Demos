using System;
using System.Threading.Tasks;

namespace Demos
{
    public abstract class NotificationBaseWatcher : IDisposable
    {
        protected CancellationTokenSource revoker;
        protected bool disposed = false;
        protected readonly Logger logger;

        private readonly BuildingAccessRepository buildingAccessRepository;
        protected readonly UserRepository userRepository;

        protected IUpdateSenderProxy updateSenderProxy;
        protected IWebSocketsInterface webSocketsInterface;

        protected NotificationBaseWatcher(Logger logger,
            IUpdateSenderProxy updateSenderProxy, IWebSocketsInterface webSocketsInterface,
            UserRepository userRepository, BuildingAccessRepository buildingAccessRepository)
        {
            this.userRepository = userRepository; this.buildingAccessRepository = buildingAccessRepository;
            this.logger = logger;
            this.updateSenderProxy = updateSenderProxy; this.webSocketsInterface = webSocketsInterface;
        }
        ~NotificationBaseWatcher()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                revoker.Cancel();
                revoker.Dispose();
            }
            disposed = true;
        }

        /// <summary>
        /// Get users with enabled notifications within building id and required access
        /// </summary>
        /// <param name="buildingId"></param>
        /// <param name="accessAnyBitFilter">One of this access</param>
        /// <returns></returns>
        protected async Task<List<MongoEntity>> getUsersWithEnabledNotificationsInBuilding(
            string buildingId, long accessAnyBitFilter)
        {
            var usersInBuilding = await buildingAccessRepository.getRelatedUsersInBuilding(buildingId, accessAnyBitFilter);
            var usersInBuildingWithNotifications = await userRepository.filterUsersWithEnabledNotifications(
                usersInBuilding.Select(x => x._id), (long)Notifications.Tickets);
            return usersInBuildingWithNotifications;
        }

        protected Task cofigureWatcher<WatchType>(
            IMongoCollection<WatchType> collectionToMonitor,
            Func<ChangeStreamDocument<WatchType>, Task> watchFunction,
            ChangeStreamOperationType operationType,
            ChangeStreamFullDocumentOption changeStreamDocumentOption = ChangeStreamFullDocumentOption.UpdateLookup,
            byte secondsToWait = 2)
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<WatchType>>().Match(x => x.OperationType == operationType);
            return buildWatcher(pipeline, changeStreamDocumentOption, secondsToWait, watchFunction, collectionToMonitor);
        }

        private async Task buildWatcher<WatchType>(
            PipelineDefinition<ChangeStreamDocument<WatchType>, ChangeStreamDocument<WatchType>> pipeline,
            ChangeStreamFullDocumentOption changeStreamDocumentOption,
            byte secondsToWait,
            Func<ChangeStreamDocument<WatchType>, Task> watchFunction,
            IMongoCollection<WatchType> collectionToMonitor)
        {
            var options = new ChangeStreamOptions()
            {
                FullDocument = changeStreamDocumentOption,
                MaxAwaitTime = TimeSpan.FromSeconds(secondsToWait)
            };
            BsonDocument resumer = null;
            while (!revoker.IsCancellationRequested)
            {
                try
                {
                    if (resumer != null) options.ResumeAfter = resumer;
                    using var cursor = await collectionToMonitor.WatchAsync(pipeline, options, revoker.Token);
                    await cursor.ForEachAsync(watchFunction, revoker.Token);
                    resumer = cursor.GetResumeToken();
                }
                catch (Exception ex)
                {
                    logger.Fatal(ex);
                }
            }
        }

        protected Task cofigureWatcher<WatchType>(
            IMongoCollection<WatchType> collectionToMonitor,
            Func<ChangeStreamDocument<WatchType>, Task> watchFunction,
            string watchFilter,
            ChangeStreamFullDocumentOption changeStreamDocumentOption = ChangeStreamFullDocumentOption.UpdateLookup,
            byte secondsToWait = 2)
        {
            var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<WatchType>>().Match(watchFilter);
            return buildWatcher(pipeline, changeStreamDocumentOption, secondsToWait, watchFunction, collectionToMonitor);
        }

        /// <summary>
        /// Generic method for revoking updates by _id
        /// </summary>
        /// <param name="deletedEntity"></param>
        /// <returns></returns>
        protected Task processNotificableEntityWasDeleted<EntityType>(ChangeStreamDocument<EntityType> deletedEntity)
        {
            var deletedEntityId = deletedEntity.DocumentKey["_id"].ToString();
            if (deletedEntityId == null)
            {
                logger.Fatal($"document without id was received", deletedEntity);
                return Task.CompletedTask;
            }
            return updateSenderProxy.revokeNotificationForObject(deletedEntityId);
        }
    }
    /// <summary>
    /// Continiously watching ticket collection and sending notification to required entities
    /// </summary>
    public sealed class TicketNotificationWatcher : NotificationBaseWatcher
    {
        private readonly ApartmentsRepository apartmentsRepository;
        private readonly TicketsRepository ticketsRepository;
        private readonly PositionsRepository positionsRepository;

        //private readonly Task insertWatcher;
        //private readonly Task updateWatcher;
        //private readonly Task deleteWatcher;

        private readonly Task globalWatcher;

        public TicketNotificationWatcher(
            ApartmentsRepository apartmentsRepository, BuildingAccessRepository buildingAccessRepository,
            TicketsRepository ticketsRepository, UserRepository userRepository,
            PositionsRepository positionsRepository,
            IUpdateSenderProxy updateSenderProxy, IWebSocketsInterface websocketsInterface,
            Logger logger)
            : base(logger, updateSenderProxy, websocketsInterface, userRepository, buildingAccessRepository)
        {
            this.apartmentsRepository = apartmentsRepository; this.ticketsRepository = ticketsRepository;
            this.positionsRepository = positionsRepository;
            if (revoker == null)
            {
                revoker = new CancellationTokenSource();
                //insertWatcher = watchInsertCollection();
                //updateWatcher = watchTicketUpdated();
                //deleteWatcher = watchTicketDeleted();
                globalWatcher = globalWatch();
            }
        }

        private IMongoCollection<TicketModel> getCollection()
        {
            return ticketsRepository.getCollection<TicketModel>().WithReadPreference(ReadPreference.Secondary).WithReadConcern(ReadConcern.Majority);
        }

        private Task globalWatch()
        {
            string filter = "{ operationType: { $in: [ 'replace', 'insert', 'update', 'delete' ] } }";
            return cofigureWatcher(getCollection(), brancher, filter, ChangeStreamFullDocumentOption.UpdateLookup, 5);
        }

        /// <summary>
        /// Launch required action depending on performed action (delete/insert/update)
        /// </summary>
        /// <param name="ticket"></param>
        /// <returns></returns>
        private Task brancher(ChangeStreamDocument<TicketModel> ticket)
        {
            switch (ticket.OperationType)
            {
                case ChangeStreamOperationType.Delete:
                    return processNotificableEntityWasDeleted(ticket);
                case ChangeStreamOperationType.Insert:
                    return processTicketInsert(ticket);
                case ChangeStreamOperationType.Update:
                case ChangeStreamOperationType.Replace:
                    return processTicketUpdated(ticket);
                default:
                    logger.Fatal($"{nameof(ticket.OperationType)} is not implemented");
                    return Task.CompletedTask;
            }
        }

        #region Ticket Creation

        /// <summary>
        /// Stage A.0: Init insert watcher
        /// </summary>
        /// <returns></returns>
        private Task watchInsertCollection()
        {
            return cofigureWatcher(getCollection(), processTicketInsert, ChangeStreamOperationType.Insert,
                ChangeStreamFullDocumentOption.UpdateLookup, 5);
        }

        /// <summary>
        /// Stage A.1: Process new ticket
        /// </summary>
        /// <param name="newTicket"></param>
        /// <returns></returns>
        private Task processTicketInsert(ChangeStreamDocument<TicketModel> newTicket)
        {
            bool isAdminCompetency = (newTicket.FullDocument.positionId == null);
            if (isAdminCompetency)
                return sendNotificationsToAdmins(newTicket.FullDocument);
            else
                return sendNotificationsToPositionsOwners(newTicket.FullDocument);
        }

        /// <summary>
        /// Stage A.1.1: Send not targeted ticket to building administrators
        /// </summary>
        /// <param name="createdTicket"></param>
        /// <returns></returns>
        private async Task sendNotificationsToAdmins(TicketModel createdTicket)
        {
            var buildingId = (await apartmentsRepository.getBuildingIdByApartId(createdTicket.apartId)).buildingId;
            var adminMask = AccessMaskHelper.buildMask(sAdmin: true, admin: true);
            var adminsWithNotification = await getUsersWithEnabledNotificationsInBuilding(buildingId, adminMask);
            adminsWithNotification.AsParallel().ForAll(user => sendTicketCreatedNotification(createdTicket, buildingId, user));
        }

        /// <summary>
        /// Stage A.1.2: Send targeted ticket to staff which assigned to certain position
        /// </summary>
        /// <param name="createdTicket"></param>
        /// <returns></returns>
        private async Task sendNotificationsToPositionsOwners(TicketModel createdTicket)
        {
            if (createdTicket.positionId == null)
            {
                logger.Fatal($"{nameof(TicketNotificationWatcher)} wtf, impossible ticket passed into func `sendNotificationsToPositionsOwners`");
                return;
            } // Thread should continue
            var users = await positionsRepository.getPersonalForCertainPosition(createdTicket.positionId);
            var buildingId = (await apartmentsRepository.getBuildingIdByApartId(createdTicket.apartId)).buildingId;
            if (users.relatedPersonal.Length > 0)
            {
                var usersWithEnabledNotifications = await userRepository.filterUsersWithEnabledNotifications(
                    users.relatedPersonal, (long)Notifications.Tickets);
                usersWithEnabledNotifications.AsParallel().WithDegreeOfParallelism(4)
                    .ForAll(user => sendTicketCreatedNotification(createdTicket, buildingId, user));
            }
        }

        private void sendTicketCreatedNotification(TicketModel createdTicket, string buildingId, IMongoEntity user)
        {
            var update = new UpdateModel()
            {
                updateType = UpdateType.TicketCreated,
                parentId = createdTicket._id,
                message = $"New ticket created: {createdTicket.header}",
                buildingId = buildingId,
                updateOwnerId = user._id
            };
            updateSenderProxy.sendNotificationsToCertainUserSync(update, null);
        }

        #endregion

        #region Ticket Updating

        /// <summary>
        /// Stage B.0: Init update watcher
        /// </summary>
        /// <returns></returns>
        private Task watchTicketUpdated()
        {
            string filter = "{ operationType: { $in: [ 'replace', 'update' ] } }";
            return cofigureWatcher(getCollection(), processTicketUpdated, filter, ChangeStreamFullDocumentOption.UpdateLookup, 5);
        }

        /// <summary>
        /// Stage B.1: Branch algo for choosing required notification type
        /// </summary>
        /// <param name="updatedTicket"></param>
        /// <returns></returns>
        private async Task processTicketUpdated(ChangeStreamDocument<TicketModel> updatedTicket)
        {
            if (updatedTicket.OperationType == ChangeStreamOperationType.Update
                || updatedTicket.OperationType == ChangeStreamOperationType.Replace)
            {
                var buildingId = (await apartmentsRepository.getBuildingIdByApartId(updatedTicket.FullDocument.apartId)).buildingId;
                switch (updatedTicket.FullDocument.status)
                {
                    case TicketModel.TicketStatus.Rejected:
                        await sendTicketRejectedStatus(updatedTicket.FullDocument, buildingId);
                        return;
                    case TicketModel.TicketStatus.Finished:
                        await sendTicketFinishedStatus(updatedTicket.FullDocument, buildingId);
                        return;
                    case TicketModel.TicketStatus.Assigned:
                        await sendTicketInProgress(updatedTicket.FullDocument, buildingId, true);
                        await sendTicketInProgress(updatedTicket.FullDocument, buildingId);
                        return;
                    case TicketModel.TicketStatus.Reviewed:
                    case TicketModel.TicketStatus.Closed:
                        await sendTicketInProgress(updatedTicket.FullDocument, buildingId);
                        return;
                    default:
                        logger.Warn($"Ticket status {nameof(updatedTicket.FullDocument.status)} is not supported in `processTicketUpdated`");
                        return;
                }
            }
        }

        /// <summary>
        /// Stage B.1.1: Sending ticket is finished update
        /// </summary>
        /// <param name="updatedTicket"></param>
        /// <param name="buildingId"></param>
        /// <returns></returns>
        private async Task sendTicketFinishedStatus(TicketModel updatedTicket, string buildingId)
        {
            if (updatedTicket.executerId == null)
            {
                logger.Fatal($"WTF, how ticket w/o {nameof(updatedTicket.executerId)} passed into `sendTicketFinishedStatus`");
                return; // base thread should not be ended
            }
            var update = new UpdateModel()
            {
                buildingId = buildingId,
                updateOwnerId = updatedTicket.executerId,
                updateType = UpdateType.TicketStatusChanged,
                parentId = updatedTicket._id,
                message = $"Ticket {updatedTicket.header} closed with mark {updatedTicket.feedback.mark}"
            };
            await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
        }

        /// <summary>
        /// Stage B.1.2: Sending ticket in progress update (reviewed, assigned, closed)
        /// Author`s related notifications
        /// </summary>
        /// <param name="updatedTicket"></param>
        /// <param name="buildingId"></param>
        /// <param name="isAssigned"></param>
        /// <returns></returns>
        private async Task sendTicketInProgress(TicketModel updatedTicket, string buildingId, bool isAssigned = false)
        {
            string newStatus = "unknown";
            switch (updatedTicket.status)
            {
                case TicketModel.TicketStatus.Reviewed:
                    newStatus = "reviewed";
                    break;
                case TicketModel.TicketStatus.Assigned:
                    newStatus = "assigned";
                    break;
                case TicketModel.TicketStatus.Closed:
                    newStatus = "closed";
                    break;
                default:
                    logger.Warn($"Status {nameof(updatedTicket.status)} is not supported `sendTicketInProgress`");
                    break;
            }
            var update = new UpdateModel()
            {
                buildingId = buildingId,
                updateOwnerId = updatedTicket.executerId,
                updateType = UpdateType.TicketStatusChanged,
                parentId = updatedTicket._id,
                message = $"Ticket {updatedTicket.header} changed status to {newStatus}" // TODO: Translate
            };
            await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
            if (isAssigned)
            {
                if (updatedTicket.executerId == null)
                {
                    logger.Fatal($"`sendTicketInProgress` isAssigned was true, but there is no executer",
                        JsonConvert.SerializeObject(updatedTicket, Formatting.Indented));
                    return;
                }
                update.updateType = UpdateType.TicketWasAssignedToYou;
                update.updateOwnerId = updatedTicket.executerId; update.message = $"You was assigned to {updatedTicket.header}";
                await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
            }
        }

        /// <summary>
        /// Stage B.1.3: Sending notifications / updates for rejected ticket
        /// </summary>
        /// <param name="updatedTicket"></param>
        /// <param name="buildingId"></param>
        /// <returns></returns>
        private async Task sendTicketRejectedStatus(TicketModel updatedTicket, string buildingId)
        {
            var additionalMessage = (updatedTicket.rejectionComment != null) ?
                $" with message {updatedTicket.rejectionComment}" : string.Empty;
            var update = new UpdateModel()
            {
                buildingId = buildingId,
                updateOwnerId = updatedTicket.authorId,
                updateType = UpdateType.TicketStatusChanged,
                parentId = updatedTicket._id,
                message = $"Ticket {updatedTicket.header} was rejected{additionalMessage}"
            };
            await updateSenderProxy.sendNotificationsToCertainUserAsync(update, null);
        }

        #endregion

        #region Ticket cancelled / deleted

        /// <summary>
        /// Stage C.0: Revoke updates / notification, if ticket was deleted / cancelled
        /// </summary>
        /// <returns></returns>
        private Task watchTicketDeleted()
        {
            return cofigureWatcher(getCollection(), processNotificableEntityWasDeleted<TicketModel>,
                ChangeStreamOperationType.Delete, ChangeStreamFullDocumentOption.Default, 5);
        }

        #endregion

        protected override void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                //insertWatcher.Dispose();
                //deleteWatcher.Dispose();
                //updateWatcher.Dispose();
                globalWatcher.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
