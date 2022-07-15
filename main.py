import random
import key


class Compare:
    def compareMetadata(self, dict1, dict2):
        for k, val in dict1.items():
            if k not in dict2 or dict2[k] != val:
                return False
        return True

    def compareTags(self, list1, list2):
        for dict1 in list1:
            if dict1 not in list2:
                return False
        return True


class S3(Compare):
    s3 = key.session.resource('s3')
    s3_a = key.session.client('s3')

    metadata1 = ['1999', '2002', '2005']
    metadata2 = ['ram', 'jon', 'shubham']
    tag1 = ['true', 'false']
    tag2 = ['public', 'protected', 'private']

    def putObjects(self, noOfObj):
        for i in range(noOfObj):
            object = self.s3.Object('task-second', 'file_name' + str(i) + '.csv')
            result = object.put(Body=open('C:\\Users\Amit\Downloads\students data.csv', 'rb'),
                                Metadata={
                                    "year": self.metadata1[random.randint(0, 2)],
                                    "author": self.metadata2[random.randint(0, 2)]},
                                Tagging='pii_data' + '=' + self.tag1[random.randint(0, 1)] + '&' + 'security' + '=' +
                                        self.tag2[random.randint(0, 2)]
                                )
            res = result.get('ResponseMetadata')
            if res.get('HTTPStatusCode') == 200:
                print("File no. " + str(i) + "Uploaded Successfully")
            else:
                print("File no. " + str(i) + "Failed to Upload")

    def DeleteObjsBasedOnMetdataTags(self, filterMetadata, filterTags):
        bucket = self.s3.Bucket('task-second')
        objectsToDelete = []
        for i in bucket.objects.all():
            object = bucket.Object(i.key)
            objTags = self.s3_a.get_object_tagging(Bucket=bucket.name, Key=object.key)['TagSet']
            if Compare.compareMetadata(self, filterMetadata, object.metadata) and Compare.compareTags(self, filterTags, objTags):
                objectsToDelete.append({'Key': object.key, 'VersionId': 'null'})
        response = None
        print(objectsToDelete)
        if objectsToDelete:
            response = bucket.delete_objects(Delete={'Objects': objectsToDelete})
        return response


class Main(S3):
    def main(self):
        choice = 0
        while choice != 3:
            choice = int(input("1:Upload objects with tags and metadata\n2:Delete objects with tags\n3:Exit\n"))
            if choice == 1:
                n = int(input("Enter number of objects:\n"))
                S3.putObjects(self, n)

            elif choice == 2:
                response = S3.DeleteObjsBasedOnMetdataTags(self,
                                                           {'year': '1999', 'author': 'ram'},
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


if __name__ == "__main__":
    m = Main()
    m.main()
