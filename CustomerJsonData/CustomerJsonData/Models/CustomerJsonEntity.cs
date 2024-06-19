namespace CustomerJsonData.Models
{
    public class CustomerJsonEntity
    {
        public string type { get; set; }
        public string message { get; set; }
        public List<Payload> payload { get; set; }
        public string status { get; set; }
        public string id { get; set; }
       
        
    }
}
