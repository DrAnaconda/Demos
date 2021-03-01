using System;
using System.Collections.Generic;
using System.Text;

namespace Demos
{
    public static class AccessMaskHelper
    {
        static public long buildMask(bool sAdmin = false, bool admin = false, bool staff = false, bool user = false)
        {
            long mask = 0;
            if (sAdmin) mask += (long)GlobalRoles.SAdmin;
            if (admin) mask += (long)GlobalRoles.Admin;
            if (staff) mask += (long)GlobalRoles.Staff;
            if (user) mask += (long)GlobalRoles.User;
            return mask;
        }
        public static bool bitmaskAnyBitSet(this long contextAccess, long requiredAccess)
        {
            if (contextAccess < 0 || requiredAccess < 0)
            {
                throw new InvalidOperationException($"You should not pass negative values for this operation");
            }
            return ((contextAccess & requiredAccess) > 0);
        }
        private static bool throwForFailedCheck(IEnumerable<string> failedEntitiesIds, long failedRequiredAccess, bool throwOnFailure)
        {
            var uid = OneConnectionScopedData.getCurrentConnectionUser().getUid(); // DI, user data, scoped per request
            var message = $"User {uid} have no access to entities: {string.Join(',', failedBuildingsIds)} with one of access type {failedRequiredAccess}";
            return throwOnFailure ? throw new ApiException(message, 403) : false;
        }
        public static bool checkAccessToBuildings(long requiredOneOfAccess, bool throwOnFailure, params string[] requiredBuildings)
        {
            if (requiredBuildings == null)
            {
                throw new ArgumentNullException(nameof(requiredBuildings));
            }
            var userAccess = OneConnectionScopedData.getCurrentConnectionUser().session.accessVector; // DI, scoped per request
            var failedChecks = new List<string>(requiredBuildings.Length);
            foreach (var requiredBuilding in requiredBuildings)
            {
                var buildingAccess = userAccess.FirstOrDefault(x => x.buildingId == requiredBuilding);
                if (buildingAccess == null)
                {
                    failedChecks.Add(requiredBuilding);
                }
                else
                {
                    if (!buildingAccess.accessMask.bitmaskAnyBitSet(requiredOneOfAccess))
                        failedChecks.Add(requiredBuilding);
                }
            }
            return failedChecks?.Count > 0 ? throwForFailedCheck(failedChecks, requiredOneOfAccess, throwOnFailure) : true;
        }
    }
}