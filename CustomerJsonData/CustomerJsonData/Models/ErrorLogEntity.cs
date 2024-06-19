namespace CustomerJsonData.Models
{
    public class ErrorLogEntity
    {
        public string PipeLineName { get; set; }
        public string FileName { get; set; }
        public string ParentNodeName { get; set; }
        public string ErrorMessage { get; set; }
    }
}
