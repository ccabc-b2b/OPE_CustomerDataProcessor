using CustomerJsonData.Models;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using ErrorEventArgs = Newtonsoft.Json.Serialization.ErrorEventArgs;

namespace CustomerJsonData
{
    public class CustomerJsonData
    {
        private readonly IConfiguration _configuration;
        readonly string containerName = Properties.Settings.Default.ContainerName;
        readonly string blobDirectoryPrefix = Properties.Settings.Default.BlobDirectoryPrefix;
        readonly string destblobDirectoryPrefix = Properties.Settings.Default.DestDirectory;
        public CustomerJsonData(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        public void LoadCustomerData()
        {
            try
            {
                List<BlobEntity> blobList = new List<BlobEntity>();
                var storageKey = _configuration["StorageKey"];
                var storageAccount = CloudStorageAccount.Parse(storageKey);
                var myClient = storageAccount.CreateCloudBlobClient();
                var container = myClient.GetContainerReference(containerName);
                var list = container.ListBlobs().OfType<CloudBlobDirectory>().ToList();
                if (list != null && list.Count > 0)
                {
                    var blobListDirectory = list[0].ListBlobs().OfType<CloudBlobDirectory>().ToList();
                    foreach (var blobDirectory in blobListDirectory)
                    {
                        if (blobDirectory.Prefix == blobDirectoryPrefix)
                        {
                            foreach (var blobFile in blobDirectory.ListBlobs().OfType<CloudBlockBlob>())
                            {
                                BlobEntity blobDetails = new BlobEntity();
                                string[] blobName = blobFile.Name.Split(new char[] { '/' });
                                string[] filename = blobName[2].Split(new char[] { '.' });
                                string[] fileDateTime = filename[0].Split(new char[] { '_' });
                                string fileCreatedDateTime = fileDateTime[1] + fileDateTime[2];
                                string formatString = "yyyyMMddHHmmss";
                                CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobFile.Name);
                                blobDetails.Blob = blockBlob;
                                blobDetails.FileName = blobName[2];
                                blobDetails.FileCreatedDate = DateTime.ParseExact(fileCreatedDateTime, formatString, null);
                                blobDetails.FileData = blockBlob.DownloadTextAsync().Result;
                                blobDetails.BlobName = blobFile.Name;
                                blobList.Add(blobDetails);
                            }
                            blobList.OrderByDescending(x => x.FileCreatedDate.Date).ThenByDescending(x => x.FileCreatedDate.TimeOfDay).ToList();
                        }
                    }

                    foreach (var blobDetails in blobList)
                    {
                        CheckRequiredFields(blobDetails, container);
                    }
                }
                else
                {
                    var errorLog = new ErrorLogEntity();
                    errorLog.PipeLineName = "Customer";
                    errorLog.ErrorMessage = "No source folder present in the container";
                    //SaveErrorLogData(errorLog);
                    Logger logger = new Logger(_configuration);
                    logger.ErrorLogData(null, errorLog.ErrorMessage);
                }
            }
            catch (StorageException ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Customer";
                errorLog.ErrorMessage = ex.Message;
                //SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
            catch (Exception ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Customer";
                errorLog.ErrorMessage = ex.Message;
                //SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
        }
        private void CheckRequiredFields(BlobEntity blobDetails, CloudBlobContainer container)
        {
            try
            {
                List<string> errors = new List<string>();
                if (string.IsNullOrEmpty(blobDetails.FileData))
                {
                    blobDetails.Status = "Error";
                    var errorLog = new ErrorLogEntity();
                    errorLog.PipeLineName = "Customer";
                    errorLog.FileName = blobDetails.FileName;
                    errorLog.ErrorMessage = "File is empty";
                    Logger logger = new Logger(_configuration);
                    logger.ErrorLogData(null, "File is empty");
                }
                else
                {
                    CustomerJsonEntity customerdataList = JsonConvert.DeserializeObject<CustomerJsonEntity>(blobDetails.FileData, new JsonSerializerSettings
                    {
                        Error = delegate (object sender, ErrorEventArgs args)
                        {
                                errors.Add(args.ErrorContext.Error.Message);
                                args.ErrorContext.Handled = true;

                        },
                        Converters = { new IsoDateTimeConverter() }
                    });
                    Dictionary<string, int> returnData = new Dictionary<string, int>();
                    if (customerdataList == null)
                    {
                        returnData.Add("Customer", 0);
                        var errorLog = new ErrorLogEntity();
                        errorLog.PipeLineName = "Customer";
                        errorLog.FileName = blobDetails.FileName;
                        errorLog.ErrorMessage = errors[0];
                        //SaveErrorLogData(errorLog);
                        Logger logger = new Logger(_configuration);
                        logger.ErrorLogData(null, errors[0]);
                    }
                    else
                    {
                        foreach (var payLoad in customerdataList.payload)
                        {
                            if (string.IsNullOrEmpty(payLoad.CustomerNumber))
                            {
                                returnData.Add("CustomerNumber is null", 0);
                            }
                            else if (payLoad.CustomerSalesArea == null)
                            {
                                returnData.Add("CustomerSalesArea is null", 0);
                            }
                            else if (payLoad.CustomerSalesArea.Count == 0)
                            {
                                returnData.Add("CustomerSalesArea is null", 0);
                            }
                            else
                            {
                                foreach (var salesArea in payLoad.CustomerSalesArea)
                                {
                                    if (string.IsNullOrEmpty(salesArea.SalesOrg))
                                    {
                                        returnData.Add("SalesOrg is null", 0);
                                    }
                                    else if (salesArea.DistributionChannel == null)
                                    {
                                        returnData.Add("DistributionChannel is null", 0);
                                    }
                                    else if (salesArea.Division == null)
                                    {
                                        returnData.Add("Division is null", 0);
                                    }
                                    else
                                    {
                                        var customerDBEntity = new CustomerDBEntity();
                                        customerDBEntity.CustomerNumber = payLoad.CustomerNumber;
                                        customerDBEntity.CompanyCode = payLoad.CompanyCode;
                                        customerDBEntity.IndustryCode1 = payLoad.KeyAccount;
                                        customerDBEntity.BusinessTypeId = payLoad.BusinessTypeId;
                                        customerDBEntity.CustomerGroup = salesArea.CustomerGroup;
                                        customerDBEntity.DistributionChannel = salesArea.DistributionChannel;
                                        customerDBEntity.Division = salesArea.Division;
                                        customerDBEntity.PaymentTerms = salesArea.PaymentTerms;
                                        customerDBEntity.SalesGroup = salesArea.SalesGroup;
                                        customerDBEntity.SalesOffice = salesArea.SalesOffice;
                                        customerDBEntity.SalesOrg = salesArea.SalesOrg;
                                        customerDBEntity.CustomerClass = payLoad.CustomerClass;
                                        customerDBEntity.CustomerSubTrade = payLoad.CustomerSubTrade;
                                        customerDBEntity.DeliveringPlant = payLoad.DeliveringPlant;
                                        customerDBEntity.SalesRoute = salesArea.SalesRoute;
                                        customerDBEntity.SalesRepId = payLoad.SalesRepId;
                                        customerDBEntity.ExportCountryCode = payLoad.ExportCountryCode;
                                        customerDBEntity.Region = payLoad.Region;
                                        customerDBEntity.CustomerPriceGroup = salesArea.CustomerPriceGroup;
                                        customerDBEntity.PricingProcedure = payLoad.PricingProcedure;
                                        customerDBEntity.PriceListType = payLoad.PriceList;
                                        customerDBEntity.TaxClassification = salesArea.TaxClassification;
                                        customerDBEntity.SalesDistrict = salesArea.SalesDistrict;
                                        customerDBEntity.IncoTerms1 = payLoad.IncoTerms1;
                                        customerDBEntity.TaxCountry = payLoad.TaxCountry;
                                        customerDBEntity.TaxCategory = payLoad.TaxCategory;
                                        customerDBEntity.IndustryKey = payLoad.IndustryKey;
                                        customerDBEntity.PartnerNumber = payLoad.PartnerNumber;
                                        customerDBEntity.IsDeleted = salesArea.IsDeleted;
                                        customerDBEntity.SalesPolicyId = payLoad.SalesPolicyId;
                                        customerDBEntity.BottlerTr = payLoad.TradeChannel;
                                        customerDBEntity.POType = payLoad.POType;
                                        if (string.IsNullOrEmpty(salesArea.IsDeleted))
                                        {
                                            customerDBEntity.IsDeleted = "N";
                                        }
                                        else
                                        {
                                            customerDBEntity.IsDeleted = salesArea.IsDeleted;
                                        }
                                        var return_Customer = SaveCustomerData(customerDBEntity);
                                        returnData.Add("Customer" + payLoad.CustomerNumber, return_Customer);
                                    }
                                }
                            }
                        }
                    }
                    foreach (var returnvalue in returnData)
                    {
                        if (returnvalue.Value == 0)
                        {
                            blobDetails.Status = "Error";
                            var errorLog2 = new ErrorLogEntity();
                            errorLog2.PipeLineName = "Customer";
                            errorLog2.FileName = blobDetails.FileName;
                            errorLog2.ParentNodeName = returnvalue.Key;
                            //SaveErrorLogData(errorLog2);
                            break;
                        }
                        else
                        {
                            blobDetails.Status = "Success";
                        }
                    }
                }
                var destDirectory = destblobDirectoryPrefix + DateTime.Now.Year + "/" + DateTime.Now.Month + "/" + DateTime.Now.Day;
                MoveFile(blobDetails, container, destDirectory);
            }
            catch (Exception ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Customer";
                errorLog.ParentNodeName = "CheckRequiredFields";
                errorLog.ErrorMessage = ex.Message;
                //SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
        }
        private int SaveCustomerData(CustomerDBEntity customerdata)
        {
            using (SqlConnection connection = new SqlConnection(_configuration["DatabaseConnectionString"]))
            {
                try
                {
                    SqlCommand cmd = new SqlCommand("Customer_save", connection);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@CustomerNumber", customerdata.CustomerNumber);
                    cmd.Parameters.AddWithValue("@CompanyCode", customerdata.CompanyCode);
                    cmd.Parameters.AddWithValue("@IndustryCode1", customerdata.IndustryCode1);
                    cmd.Parameters.AddWithValue("@BusinessTypeId", customerdata.BusinessTypeId);
                    cmd.Parameters.AddWithValue("@CustomerGroup", customerdata.CustomerGroup);
                    cmd.Parameters.AddWithValue("@DistributionChannel", customerdata.DistributionChannel);
                    cmd.Parameters.AddWithValue("@Division", customerdata.Division);
                    cmd.Parameters.AddWithValue("@PaymentTerms", customerdata.PaymentTerms);
                    cmd.Parameters.AddWithValue("@SalesGroup", customerdata.SalesGroup);
                    cmd.Parameters.AddWithValue("@SalesOffice", customerdata.SalesOffice);
                    cmd.Parameters.AddWithValue("@SalesOrg", customerdata.SalesOrg);
                    cmd.Parameters.AddWithValue("@CustomerClass", customerdata.CustomerClass);
                    cmd.Parameters.AddWithValue("@CustomerSubTrade", customerdata.CustomerSubTrade);
                    cmd.Parameters.AddWithValue("@DeliveringPlant", customerdata.DeliveringPlant);
                    cmd.Parameters.AddWithValue("@SalesRoute", customerdata.SalesRoute);
                    cmd.Parameters.AddWithValue("@SalesRepId", customerdata.SalesRepId);
                    cmd.Parameters.AddWithValue("@ExportCountryCode", customerdata.ExportCountryCode);
                    cmd.Parameters.AddWithValue("@Region", customerdata.Region);
                    cmd.Parameters.AddWithValue("@CustomerPriceGroup", customerdata.CustomerPriceGroup);
                    cmd.Parameters.AddWithValue("@PricingProcedure", customerdata.PricingProcedure);
                    cmd.Parameters.AddWithValue("@PriceListType", customerdata.PriceListType);
                    cmd.Parameters.AddWithValue("@TaxClassification", customerdata.TaxClassification);
                    cmd.Parameters.AddWithValue("@SalesDistrict", customerdata.SalesDistrict);
                    cmd.Parameters.AddWithValue("@IncoTerms1", customerdata.IncoTerms1);
                    cmd.Parameters.AddWithValue("@TaxCountry", customerdata.TaxCountry);
                    cmd.Parameters.AddWithValue("@TaxCategory", customerdata.TaxCategory);
                    cmd.Parameters.AddWithValue("@IndustryKey", customerdata.IndustryKey);
                    cmd.Parameters.AddWithValue("@PartnerNumber", customerdata.PartnerNumber);
                    cmd.Parameters.AddWithValue("@IsDeleted", customerdata.IsDeleted);
                    cmd.Parameters.AddWithValue("@SalesPolicyId", customerdata.SalesPolicyId);
                    cmd.Parameters.AddWithValue("@BottlerTr", customerdata.BottlerTr);
                    cmd.Parameters.AddWithValue("@POType", customerdata.POType);
                    cmd.Parameters.Add("@returnObj", SqlDbType.BigInt).Direction = ParameterDirection.Output;
                    connection.Open();
                    int retval = cmd.ExecuteNonQuery();
                    connection.Close();
                    if (retval != 0)
                    {
                        return retval;
                    }
                    else
                    {
                        return 0;
                    }

                }
                catch (Exception ex)
                {
                    var errorLog = new ErrorLogEntity();
                    errorLog.PipeLineName = "Customer";
                    errorLog.ParentNodeName = "Customer Save";
                    errorLog.ErrorMessage = ex.Message;
                    //SaveErrorLogData(errorLog);
                    Logger logger = new Logger(_configuration);
                    logger.ErrorLogData(ex, ex.Message);
                }
            }
            return 0;
        }
        public void MoveFile(BlobEntity blob, CloudBlobContainer destContainer, string destDirectory)
        {
            CloudBlockBlob destBlob;
            try
            {
                if (blob.Blob == null)
                    throw new Exception("Source blob cannot be null.");

                if (!destContainer.Exists())
                    throw new Exception("Destination container does not exist.");

                string name = blob.FileName;
                if (destDirectory != "" && blob.Status == "Success")
                    destBlob = destContainer.GetBlockBlobReference(destDirectory + "\\Success\\" + name);
                else
                    destBlob = destContainer.GetBlockBlobReference(destDirectory + "\\Error\\" + name);

                destBlob.StartCopy(blob.Blob);
                blob.Blob.Delete();
            }
            catch (Exception ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Customer";
                errorLog.FileName = blob.FileName;
                errorLog.ParentNodeName = "Customer move";
                errorLog.ErrorMessage = ex.Message;
                //SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
        }
        public void SaveErrorLogData(ErrorLogEntity errorLogData)
        {
            using (SqlConnection con = new SqlConnection(_configuration["DatabaseConnectionString"]))
            {
                try
                {
                    SqlCommand cmd = new SqlCommand("ErrorLogDetails_save", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("@PipeLineName", errorLogData.PipeLineName);
                    cmd.Parameters.AddWithValue("@FileName", errorLogData.FileName);
                    cmd.Parameters.AddWithValue("@ParentNodeName", errorLogData.ParentNodeName);
                    cmd.Parameters.AddWithValue("@ErrorMessage", errorLogData.ErrorMessage);
                    con.Open();
                    cmd.ExecuteNonQuery();
                    con.Close();
                }
                catch (Exception)
                {

                }
            }
        }
    }
}
