namespace CustomerJsonData.Models
{
    public class Payload
    {
       
        public string CustomerNumber { get; set; }
        public string TradeName { get; set; }
        public List<CustomerEntity> CustomerSalesArea { get; set; }
        public string SalesUnit { get; set; }
        public List<Partner> Partners { get; set; }
        public string CustomerClass { get; set; }
        public string DeliveringPlant { get; set; }
        public string TradeGroup { get; set; }
        public string TradeChannel { get; set; }
        public string SubTradeChannel { get; set; }
        public string CompanyCode { get; set; }
        public string Region { get; set; }   
        public string SalesPolicyId { get; set; }
        public string IncoTerms1 { get; set; }
        public string TaxCountry { get; set; }
        public string TaxCategory { get; set; }
        public string IndustryKey { get; set; }
        public string KeyAccount { get; set; }
        public string BusinessTypeId { get; set; }
        public string CustomerSubTrade { get; set; }
        public string SalesRepId { get; set; }
        public string ExportCountryCode { get; set; }
        public string PricingProcedure { get; set; }
        public string PriceList { get; set; }
        public string PartnerNumber { get; set; }
        public string POType { get; set; }
    }
}
