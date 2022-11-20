import boto3
import sys
import json
import glob
import time
import os

# Paths and file names
POLICY_FILE_NAME = "open_policy.json"
PATH_TO_POLICY = '../config/policy/'+POLICY_FILE_NAME

#IOT Client
iot_client = boto3.client('iot')

#DynamoDb Client
dyn_client = boto3.resource('dynamodb')

#importing Table_config file
f = open('Table_config.json', 'r')
table_config = json.loads(f.read())
f.close()
table_details = table_config['Tables']

# Reading the sprinkler configuration file
f = open('sprinkler_config.json', 'r')
config = json.loads(f.read())
f.close()

# Read the config file and build the SoilSensor-Sprinkler map and Lon-Lat-Sprinkler map
sprinklers = config['sprinklers']
soil_sensors_list = []
for sprinkler in sprinklers:
    soil_sensors = sprinkler["soil_sensors"]
    for soil_sensor in soil_sensors:
        soil_sensors_list.append(soil_sensor)

def create_things():

    print("Creating things...")
    for soil_sensor in soil_sensors_list:
        response = iot_client.create_thing(thingName=soil_sensor)
    print("Things Created\n")

def iot_core_reset():
    print("Resetting the IOT_Core...")

    things = get_all_things()
    things_count = len(things["thingNames"])
    if (things_count == 0):
        print("No things found\n")

    else:
        #Detaching the principals from the things
        print(str(things_count) + " things found")
        print("Detaching principals from things...")
        for thingName in things["thingNames"]:
            associated_principals = iot_client.list_thing_principals(thingName=thingName)[
                "principals"]
            for associated_principal in associated_principals:
                iot_client.detach_thing_principal(
                    thingName=thingName, principal=associated_principal)

        # Deleting things
        for thingName in things["thingNames"]:
            iot_client.delete_thing(thingName=thingName)
        print(str(things_count) + " things deleted\n")

def get_all_things():

    # Return parameters
    thingNames = []

    # Parameters used to count things and search pages
    things_count = 0
    page_count = 0

    # Send the first request
    response = iot_client.list_things()

    while (1):
        # Increment thing count
        things_count = things_count + len(response['things'])
        for thing in response['things']:
            thingNames.append(thing['thingName'])
        # Increment Page number
        page_count += 1
        break
    return {"thingNames": thingNames}

def create_certificates():

    # Step 0: Delete the existing files under secure/keys and secure/certificates
    print("Deleting private keys ...")
    for file in glob.glob("../config/keys/private/*"):
        os.remove(file)

    print("Deleting public keys ...")
    for file in glob.glob("../config/keys/public/*"):
        os.remove(file)

    print("Deleting certificates ...")
    for file in glob.glob("../config/certificates/*"):
        os.remove(file)

    # Get things registered in the IoT core
    things = get_all_things()

    # Create certificate and keys for things
    for thing in things['thingNames']:
        # Create keys and certificates
        response = iot_client.create_keys_and_certificate(setAsActive=True)

        # Get the certificate and key contents
        certificateArn = response["certificateArn"]
        certificate = response["certificatePem"]
        key_public = response["keyPair"]["PublicKey"]
        key_private = response["keyPair"]["PrivateKey"]

        # log information
        print("Creating the certificate and key for " + thing)

        # Storing the private key
        f = open("../config/keys/private/"+thing+".pem.key", "w")
        f.write(key_private)
        f.close()

        # Storing the public key
        f = open("../config/keys/public/"+thing+".pem.key", "w")
        f.write(key_public)
        f.close()

        # Storing the certificate
        f = open("../config/certificates/"+thing+".pem.crt", "w")
        f.write(certificate)
        f.close()

def delete_all_certificates():


    # Step 0: Get the certificates
    certificates = get_all_certificates()
    certificate_count = len(certificates["certificateIds"])
    if(certificate_count == 0):
        print("No certificate found")
    else:
    # Step 1: Detach things from certificates.
        print("Detaching things from certificate...")
        for certificateArn in certificates["certificateArns"]:
            attached_things = get_all_principal_things(
                principal=certificateArn)
            for attached_thing in attached_things:
                iot_client.detach_thing_principal(
                    thingName=attached_thing, principal=certificateArn)

    # Step 2: Delete the certificates from IoT Core registery if is there any
        print("Deleting the certificate...")
        for certificateId in certificates["certificateIds"]:
            iot_client.update_certificate(
                certificateId=certificateId, newStatus='INACTIVE')
            iot_client.delete_certificate(
                certificateId=certificateId, forceDelete=True)

        print("Certificates Deleted")

def get_all_certificates():

    # Return parameters
    certificateArns = []
    certificateIds = []


    # Parameter used to count certificates and search pages
    certificates_count = 0
    page_count = 0

    # Send the first request
    response = iot_client.list_certificates()

    # Count the number of the certificates until no more certificates are present on the search pages
    while(1):
        # Increment certificate count
        certificates_count = certificates_count + len(response['certificates'])

        # Append found certificates to the lists
        for certificate in response['certificates']:
            certificateArns.append(certificate['certificateArn'])
            certificateIds.append(certificate['certificateId'])

        # Increment Page number
        page_count += 1

        # Check if nextMarker is present for next search pages
        break
    return {"certificateArns": certificateArns, "certificateIds": certificateIds}

def get_all_principal_things(principal):

    # Return parameters
    thingNames = []


    # Parameters used to count things and search pages
    things_count = 0
    page_count = 0

    # Send the first request
    response = iot_client.list_principal_things(
        principal=principal)

    # Count the number of the things until no more things are present on the search pages
    while(1):
        # Increment thing count
        things_count = things_count + len(response['things'])
        # Append found things to the lists
        for thing in response['things']:
            thingNames.append(thing)

        # Increment Page number
        page_count += 1

        # Check if nextToken is present for next search pages
        break

    return thingNames

def attach_policy_and_certificates():

    print("Attaching certificates and things ")

    thingNames = get_all_things()["thingNames"]
    certificateArns = get_all_certificates()["certificateArns"]
    policyNames = get_all_policies()["policyNames"]

    if(len(thingNames) == len(certificateArns)):
        for i in range(len(thingNames)):
            # Attach certificate to things
            iot_client.attach_thing_principal(
                thingName=thingNames[i], principal=certificateArns[i])
            print(f"\tAttaching thing {thingNames[i]} and certificate {certificateArns[i][:50]}...")

            # Attach policy to things
            iot_client.attach_principal_policy(
                policyName=policyNames[0], principal=certificateArns[i])
    else:
        print("aws-iot-core: " + "Total number of the things and certificates missmatch")

def create_policy():

    print("Creating a policy")

    # Step 0: Get the policies
    policies = get_all_policies()
    policies_count = len(policies["policyNames"])
    if(policies_count == 0):
        print("Creating a new policy")
        f = open(PATH_TO_POLICY, "r")
        policyDoc_str = f.read()
        policyName = "open_policy"
        # print(policyDoc_str)
        iot_client.create_policy(
            policyName='iot_policy', policyDocument=policyDoc_str)
        print("policy is succesfully created.")

    else:
        print("using a existing policy")

def get_all_policies():

    # Return parameters
    policyArns = []
    policyNames = []

    # Parameter used to count policies
    policy_count = 0

    # Parameters used to count policies and search pages
    policies_count = 0
    page_count = 0

    # Send the first request
    response = iot_client.list_policies()

    # Count the number of the things until no more things are present on the search pages
    while(1):
        # Increment policy count
        policies_count = policies_count + len(response['policies'])
        # Append found policies to the lists
        for policy in response['policies']:
            policyArns.append(policy['policyArn'])
            policyNames.append(policy['policyName'])

        # Increment Page number
        page_count += 1

        # Check if nextMarker is present for next search pages
        break

    return {"policyArns": policyArns, "policyNames": policyNames}

def create_table():

    for Table in table_details:
        Table_name = Table["table_name"]
        params = {
            'TableName': Table_name,
            'KeySchema': [
                {'AttributeName': Table["Partition_key"], 'KeyType': 'HASH'},
                {'AttributeName': Table["sort_key"], 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': Table["Partition_key"], 'AttributeType': 'S'},
                {'AttributeName': Table["sort_key"], 'AttributeType': 'S'}
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        }
        table = dyn_client.create_table(**params)
        print(f"Creating {Table_name}...")
        table.wait_until_exists()
        #return table

def delete_table():

    for Table in table_details:
        Table_name = Table["table_name"]
        table = dyn_client.Table(Table_name)
        if table not in dyn_client.tables.all():
            pass
        else:
            table.delete()
            print(f"Deleting {table.name}...")
            table.wait_until_not_exists()

delete_table()
create_table()
iot_core_reset()
delete_all_certificates()
create_things()
create_policy()
create_certificates()
attach_policy_and_certificates()
