# Databricks notebook source
sas_token = "sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D"
storage_account_name = "safactoreddatathon"
container_name = "source-files"
mount_point = "/mnt/azure-data-lake"

dbutils.fs.mount(
    source="wasbs://" + container_name + "@" + storage_account_name + ".blob.core.windows.net",
    mount_point=mount_point,
    extra_configs={
        "fs.azure.sas." + container_name + "." + storage_account_name + ".blob.core.windows.net": sas_token
    }
)
