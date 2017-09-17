using Amazon.Athena;
using Amazon.Athena.Model;
using athena_lambda_api.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace athena_lambda_api.Controllers
{
    class AthenaController
    {
        internal async Task<ParsedAthenaResponse> PerformAthenaQuery(string theQuery)
        {
            ParsedAthenaResponse retVal = new ParsedAthenaResponse();
            retVal.SqlQuery = theQuery;

            AmazonAthenaClient athenaClient = CreateAthenaClient();

            Console.WriteLine("Submitting Query");
            string queryId = await SubmitAthenaQuery(athenaClient, theQuery, retVal);
            Console.WriteLine(string.Format("QueryId: {0}", queryId));

            Console.WriteLine("Waiting for Query to complete");
            await WaitForQueryToComplete(athenaClient, queryId);

            Console.WriteLine("Query complete. Lets read the response!");
            await ProcessResultRows(athenaClient, queryId, retVal);

            Console.WriteLine("Query complete. Response extracted!");

            return retVal;
        }

        private AmazonAthenaClient CreateAthenaClient()
        {
            AmazonAthenaClient athenaClient = new AmazonAthenaClient(Functions.AwsAccessKeyId, Functions.AwsSecretAccessKey, Functions.DatabaseName);
            AmazonAthenaConfig config = new AmazonAthenaConfig();
            config.Timeout = new TimeSpan(0, 0, 30);

            return athenaClient;
        }

        private async Task<string> SubmitAthenaQuery(AmazonAthenaClient client, string theQuery, ParsedAthenaResponse niceAthenaResult)
        {
            // The QueryExecutionContext allows us to set the Database.
            QueryExecutionContext queryExecutionContext = new QueryExecutionContext();
            queryExecutionContext.Database = Functions.DatabaseName;

            // The result configuration specifies where the results of the query should go in S3 and encryption options
            ResultConfiguration resultConfiguration = new ResultConfiguration();
            resultConfiguration.OutputLocation = Functions.QueryOutputLocation;

            // Create the StartQueryExecutionRequest to send to Athena which will start the query.
            StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest();
            startQueryExecutionRequest.QueryString = theQuery;
            //Now reading this dynamically
            niceAthenaResult.columnOrder = new List<string>();

            startQueryExecutionRequest.QueryExecutionContext = queryExecutionContext;
            startQueryExecutionRequest.ResultConfiguration = resultConfiguration;

            var startQueryExecutionResponse = await client.StartQueryExecutionAsync(startQueryExecutionRequest);
            return startQueryExecutionResponse.QueryExecutionId;
        }

        private async Task WaitForQueryToComplete(AmazonAthenaClient client, string queryExecutionId)
        {
            GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest();
            getQueryExecutionRequest.QueryExecutionId = queryExecutionId;

            bool isQueryStillRunning = true;

            while (isQueryStillRunning)
            {
                var queryExecutionResponse = await client.GetQueryExecutionAsync(getQueryExecutionRequest);


                QueryExecutionStatus queryStatus = queryExecutionResponse.QueryExecution.Status;

                if (queryStatus.State == QueryExecutionState.FAILED)
                {
                    throw new Exception("Query Failed to run with Error Message: " + queryExecutionResponse.QueryExecution.Status.StateChangeReason);
                }
                else if (queryStatus.State == QueryExecutionState.CANCELLED)
                {
                    throw new Exception("Query was cancelled.");
                }
                else if (queryStatus.State == QueryExecutionState.SUCCEEDED)
                {
                    isQueryStillRunning = false;
                }

                // Sleep an amount before retrying again.
                Console.WriteLine("Current Status is: " + queryStatus.State.Value);
                Thread.Sleep(new TimeSpan(0, 0, 1));
            }
        }

        private async Task ProcessResultRows(AmazonAthenaClient client, string queryExecutionId, ParsedAthenaResponse niceAthenaResult)
        {
            GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest();
            // Max Results can be set but if its not set,
            // it will choose the maximum page size
            // As of the writing of this code, the maximum value is 1000
            // .withMaxResults(1000)
            getQueryResultsRequest.QueryExecutionId = queryExecutionId;

            var getQueryResultsResponse = await client.GetQueryResultsAsync(getQueryResultsRequest);

            List<ColumnInfo> columnInfoList = getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo;
            ResultSet responseData = getQueryResultsResponse.ResultSet;

            ReadHeadersFromColumnInfo(getQueryResultsResponse.ResultSet, niceAthenaResult);

            while (true)
            {
                //Convert the returned response to a serialisable JSON object
                ProcessRow_NameCounts(responseData, niceAthenaResult);

                // If the nextToken is null, there are no more pages to read. Break out of the loop.
                if (getQueryResultsResponse.NextToken == null)
                {
                    break;
                }
                getQueryResultsResponse = await client.GetQueryResultsAsync(
                    new GetQueryResultsRequest { NextToken = getQueryResultsResponse.NextToken });
                Console.WriteLine("getting more data from response...");
            }
        }

        private void ReadHeadersFromColumnInfo(ResultSet resultSet, ParsedAthenaResponse niceAthenaResult)
        {
            niceAthenaResult.columnOrder = new List<string>();
            //Get the headers ordered
            foreach (var header in resultSet.ResultSetMetadata.ColumnInfo)
            {
                niceAthenaResult.columnOrder.Add(header.Name);
            }
        }

        private void ProcessRow_NameCounts(ResultSet resultSet, ParsedAthenaResponse niceAthenaResult)
        {
            List<Row> rows = resultSet.Rows;

            //foreach (var row in resultSet.Rows)
            //{

            for (int r = 0; r < resultSet.Rows.Count; r++)
            {
                if (r == 0)
                    continue; //Row 0 has headers. Ignore me

                Dictionary<string, string> newRow = new Dictionary<string, string>();

                for (int i = 0; i < niceAthenaResult.columnOrder.Count; i++)
                {
                    newRow.Add(niceAthenaResult.columnOrder[i], resultSet.Rows[r].Data[i].VarCharValue);
                }
                niceAthenaResult.Events.Add(newRow);
            }
        }
    }
}
