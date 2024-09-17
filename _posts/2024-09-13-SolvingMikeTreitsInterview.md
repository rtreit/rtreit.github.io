---
title: Solving Mike Treit's Interview Problem
date: 2024-09-13
categories: [Technology]
tags: [coding,datascience]     
---
My brother Mike has a [favorite interview question](https://mtreit.com/programming,/interviewing/2023/02/03/InterviewQuestion.html) involving finding the common integers between 2 very large (50GB each) binary files. His requirements are highly pragmatic: he doesn't care how you get the answer, just give him a third binary file that contains the common integers by the end of the day. 

# Practical problem solving
This is my kind of problem. I'm not a highly skilled programmer and would certainly bomb most data structures and algorithm style job interviews, but in my day job as a security researcher, I am pretty good at hacking something together to find the right answer. It might not be the most elegant or efficient solution, but I can usually get the job done. Figuring out how to use the avaialble tools at my disposal to solve a problem is part of what makes my job so much fun. 

# Using what I have at hand
When I read Mike's blog post, my immediate thought was: "ok, if this was something I had to do by the end of the day, what would I actually reach for?" The answer was immediate: I'd use [PySpark](https://spark.apache.org/docs/latest/api/python/index.html).

At the time, I was doing a lot of data analysis using PySpark: mostly hunting for attack patterns in vast amounts of telemetry data, that kind of thing. Spark seemed like a logical choice for taking 2 large files and finding the common integers between them - it should just be a simple inner join on the distinct integers in each file. 

Let's do it.

# The environment
For the purposes of this blog post, I'll assume we don't have anything up and running and walk through how to get an environment stood up to use Spark to solve the problem. That way we can spin up what we need and tear it all down when we're done. 

## Operating system
I'll be running from Kali Linux in WSL2 on Windows 11. 

![KaliTerminal](assets/images/2024-09-13-MikeInterviewProblem/kaliprompt.png)

## Cloud
We'll need a Spark cluster to run our computation on. If I was really solving this problem at work I'd use an existing Azure Synapse workspace or similar, but since this is a personal project I'll spin up something myself. I'm most familiar with Microsoft Azure, so let's go with that as our cloud environment. 

### Install Azure CLI 
Let's make sure we have the commandn line tools we need for interacting with Azure. Full instructions can be found [here](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux?pivots=apt).
```bash
sudo apt-get update
sudo apt-get install azure-cli
```
> **💡 Fun Fact:** Azure CLI is written in Python 🐍

### Create a Python virtual environment
For most any project, I'll wind up using Python at some point, so I always spin up a virtual environment. 

I'm running 3.11.9 at the moment. 
```bash
python3 -m venv .venv
source .venv/bin/activate
```
### Login in Azure
```bash
az login 
```
You should get prompted to authenticate and select your Azure subscription. 
### Create a resource group
We'll create a dedicated resource group for our project that will hold all our Azure artifacts, which we can tear down when we're done. 
```bash
az group create --name mikeinterview --location westus
```
You should see:
```json
{
  "id": "/subscriptions/<yoursubscriptionid>/resourceGroups/mikeinterview",
  "location": "westus",
  "managedBy": null,
  "name": "mikeinterview",
  "properties": {
    "provisioningState": "Succeeded"
  },
  "tags": null,
  "type": "Microsoft.Resources/resourceGroups"
}
```
### Register necessary providers
```bash
az provider register --namespace Microsoft.Compute
az provider register --namespace Microsoft.Databricks
```

### Create an Azure Databricks workspace
```bash
az extension add --name databricks
az databricks workspace create --resource-group mikeinterview --name mikeinterview --location westus --sku standard
```
### Login to your Azure Databricks workspace
Browse to the Azure Portal and click on your newly created Databricks resource, then click **Launch Workspace**.
![LaunchDatabricks](assets/images/2024-09-13-MikeInterviewProblem/launchdatabricks.png)


### Add a compute cluster
Click on **Compute** and then **Create Compute**.
![LaunchDatabricks](assets/images/2024-09-13-MikeInterviewProblem/createcompute.png)

Leave the defaults but change the **Terminate after** setting to 15 minutes (to save costs).
![LaunchDatabricks](assets/images/2024-09-13-MikeInterviewProblem/createcompute-settings.png)

### Create a notebook
Click **New** -> **Notebook**
![LaunchDatabricks](assets/images/2024-09-13-MikeInterviewProblem/newnotebook.png)

In the notebook code cell, run some code. You'll be prompted to start your compute cluster. Click **Start, attach and run**.

![LaunchDatabricks](assets/images/2024-09-13-MikeInterviewProblem/startandrun.png)

The cluster should start within a few minutes and the code cell will run. 

At this point we're up and running with a cloud Spark environment we can use to try and solve the interview problem. 

## Generate the data


