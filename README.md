# dynamo
Support for AWS Dynamo drivers under the protostore crud API

Tests
-----

To run the tests edit 
  
  src/test/resources/aws_credentials.properties

to contain your AWS dynamo credentials for example:

  ACCESS_KEY: badkey
  SECRET_KEY: terrible secret
  REGION: EU_WEST_1

and run

  mvn test
