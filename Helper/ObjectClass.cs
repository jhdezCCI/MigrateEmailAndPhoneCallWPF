using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MigrateEmailAndPhoneCallWPF.UserControls
{
    public class PhoneCallRecord
    {
        public Guid ActivityId { get; set; }
        public string Subject { get; set; }
        public string Description { get; set; }
        public int? ActualDurationMinutes { get; set; }
        public string PhoneNumber { get; set; }
        public bool? DirectionCode { get; set; }
        public Guid? RegardingObjectId { get; set; }
        public DateTime? CreatedOn { get; set; }
    }
    public class EmailRecord
    {
        public Guid ActivityId { get; set; }
        public string Subject { get; set; }
        public string Description { get; set; }
        public Guid? RegardingObjectId { get; set; }
        public DateTime? CreatedOn { get; set; }
    }
    public class ActivityPartyRecord
    {
        public Guid ActivityId { get; set; }
        public Guid? PartyId { get; set; }
        public int PartyObjectTypeCode { get; set; }
        public string LogicalName { get; set; }
        public int ParticipationTypeMask { get; set; }
        public string UserEmail { get; set; }
    }
    public class UserRecord
    {
        public string Cci_op_active_directory_upn { get; set; }
        public string Fiber_crm_guid { get; set; }
        public string Msdyn_systemuser_guid { get; set; }
        public long? Cci_aim_prsn_seq_num { get; set; }
    }
    public class PublishXRef
    {
        public string Source { get; set; }
        public string Src_sys_id { get; set; }
        public string Survivor_id { get; set; }
        public string Is_Survivor { get; set; }
        public string Is_Active { get; set; }
        public string EntityType { get; set; }
    }
    public class RecordMissingInfo
    {
        private string xInfoMissing = "";
        private Guid xRecordGuid;

        public string InfoMissing
        {
            get { return xInfoMissing; }
            set { xInfoMissing = value; }
        }

        public Guid RecordGuid
        {
            get { return xRecordGuid; }
            set { xRecordGuid = value; }
        }
    }
    public class TaskRecord
    {
        public Guid ActivityId { get; set; }
        public string Subject { get; set; }
        public string Description { get; set; }
        public int? Duration { get; set; }
        public Guid? RegardingObjectId { get; set; }
        public Guid? OwnerId { get; set; }
        public DateTime? CreatedOn { get; set; }
        public int LineOfBusiness { get; set; }
        public DateTime? DueDate { get; set; }

    }
    public class AppointmentRecord
    {
        public Guid ActivityId { get; set; }
        public string Subject { get; set; }
        public string Location { get; set; }
        public int LineOfBusiness { get; set; }
        public string Description { get; set; }
        public Guid? RegardingObjectId { get; set; }
        public Guid? OwnerId { get; set; }
        public int? Duration { get; set; }
        public DateTime? ScheduleStart { get; set; }
        public DateTime? ScheduleEnd { get; set; }
        public DateTime? CreatedOn { get; set; }
        public bool? AllDayEvent { get; set; }

    }
}
