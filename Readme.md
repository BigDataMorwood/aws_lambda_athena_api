# AWS Lambda to talk to AWS Athena

AWS Athena is rather fast at querying your data straight off S3. This project allows you to query your Athena databse through AWS API/Lambda's running .NET Core and returns the results in JSON.

1) Set your AWS details in Functions.cs
2) Change the SQL in Functions.FunctionName.cs
3) Set your Lamdba's name and API path in serverless.template
4) Right-Click the project --> "Publish to AWS Lambda"
5) Check AWS API service to find the URL
6) "WOW! Athena is awesome!!"
