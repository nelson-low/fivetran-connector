## Fivetran Connectors

This submodule is for wrapping a fivetran connector with an API caller hosted on lambda.

Note this works in the lambda function if the lambda executor role has the same access to s3.

### How to use

Import the module as a sub module in the lambda function folder.

```
cd ./my-lambda-function
git submodule add <this module>.git
```

