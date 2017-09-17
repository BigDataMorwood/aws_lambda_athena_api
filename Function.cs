using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using athena_lambda_api.Controllers;
using Newtonsoft.Json;
using Amazon;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace athena_lambda_api
{
    public class Functions
    {
        //Set your values here
        public static string AwsAccessKeyId = "AddMe";
        public static string AwsSecretAccessKey = "AddMe";
        public static RegionEndpoint AwsRegion = RegionEndpoint.USWest2;
        public static string DatabaseName = "AddMe";
        public static string QueryOutputLocation = "AddMe";

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Functions()
        {
        }

        public async Task<APIGatewayProxyResponse> FunctionName(APIGatewayProxyRequest request, ILambdaContext context)
        {
            var whereClauseColumn1Value = request.QueryStringParameters["whereClauseColumn1"];
            var whereClauseColumn2Value = request.QueryStringParameters["whereClauseColumn2"];

            //Base SQL query
            string sqlQuery = @"select
                                    dimension1,
                                    dimension2
                                    sum(metric1) as Metric
                                from database.table";

            if (!string.IsNullOrEmpty(whereClauseColumn1Value))
                sqlQuery += "\nwhere dimension1 = '" + whereClauseColumn1Value + "'\n";

            if (!string.IsNullOrEmpty(whereClauseColumn2Value))
                sqlQuery += "and dimension2 = '" + whereClauseColumn2Value + "'\n";

            sqlQuery += @"group by
                                dimension1,
                                dimension2";
            
            AthenaController ac = new AthenaController();
            var athenaResponse = await ac.PerformAthenaQuery(sqlQuery);
            var responseBody = JsonConvert.SerializeObject(athenaResponse);

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };

            return response;
        }
    }
}
