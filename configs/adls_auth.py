def configure_adls(spark):

    spark.conf.set(
        "fs.azure.account.auth.type.DATALAKE_NAME.dfs.core.windows.net",
        "OAuth"
    )

    spark.conf.set(
        "fs.azure.account.oauth.provider.type.DATALAKE_NAME.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )

    spark.conf.set(
        "fs.azure.account.oauth2.client.id.DATALAKE_NAME.dfs.core.windows.net",
        "<CLIENT_ID>"
    )

    spark.conf.set(
        "fs.azure.account.oauth2.client.secret.DATALAKE_NAME.dfs.core.windows.net",
        "<CLIENT_SECRET>"
    )

    spark.conf.set(
        "fs.azure.account.oauth2.client.endpoint.DATALAKE_NAME.dfs.core.windows.net",
        "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token"
    )
