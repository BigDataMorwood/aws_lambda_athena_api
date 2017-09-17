using System.Collections.Generic;

namespace athena_lambda_api.Models
{
    public class ParsedAthenaResponse
    {
        public List<Dictionary<string, string>> Events;
        public string Events_Min;
        public string Events_Max;
        public List<string> columnOrder;
        public string SqlQuery;
        public Dictionary<string, string> WhereClauses;

        public ParsedAthenaResponse()
        {
            Events = new List<Dictionary<string, string>>();
            WhereClauses = new Dictionary<string, string>();
            columnOrder = new List<string>();
        }
    }
}
