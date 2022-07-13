import boto3
import random

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key=''
)

s3 = session.resource('s3')
s3_a = session.client('s3')

metadata1 = ['1999', '2002', '2005']
metadata2 = ['ram', 'jon', 'shubham']
tag1 = ['true', 'false']
tag2 = ['public', 'protected', 'private']


def putObjects(noOfObj):
    for i in range(noOfObj):
        object = s3.Object('second-task', 'file_name' + str(i) + '.csv')
        result = object.put(Body=open('C:\\Users\Amit\Downloads\students data1.csv', 'rb'),
                            Metadata={
                                "year": metadata1[random.randint(0, 2)],
                                "author": metadata2[random.randint(0, 2)]},
                            Tagging='pii_data' + '=' + tag1[random.randint(0, 1)] + '&' + 'security' + '=' + tag2[
                                random.randint(0, 2)]
                            )

    res = result.get('ResponseMetadata')

    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')


def compareMetadata(dict1, dict2):
    for key, val in dict1.items():
        if key not in dict2 or dict2[key] != val:
            return False
    return True


def compareTags(list1, list2):
    for dict1 in list1:
        if dict1 not in list2:
            return False
    return True


def DeleteObjsBasedOnMetdataTags(filterMetadata, filterTags):
    bucket = s3.Bucket('second-task')
    objectsToDelete = []
    for i in bucket.objects.all():
        object = bucket.Object(i.key)
        objTags = s3_a.get_object_tagging(Bucket=bucket.name, Key=object.key)['TagSet']
        if compareMetadata(filterMetadata, object.metadata) and compareTags(filterTags, objTags):
            objectsToDelete.append({'Key': object.key, 'VersionId': 'null'})
    response = None
    print(objectsToDelete)
    if objectsToDelete:
        response = bucket.delete_objects(Delete={'Objects': objectsToDelete, 'Quiet': True})
    return response


choice = 0
while choice != 3:
    choice = int(input("1:Upload objects with tags and metadata\n2:Delete objects with tags\n3:Exit\n"))
    if choice == 1:
        n = int(input("Enter number of objects:\n"))
        putObjects(n)

    elif choice == 2:
        myBucket = s3.Bucket('second-task')
        response = DeleteObjsBasedOnMetdataTags({'year': '1999', 'author': 'ram'},
                                                [{'Key': 'pii_data', 'Value': 'true'},
                                                 {'Key': 'security', 'Value': 'public'}])
        if not response or response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("successfully deleted")
        else:
            print("Error")

    elif choice == 3:
        print("Good Bye")

    else:
        print("Invalid Choice")
