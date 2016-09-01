namespace BlobStorage
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;
    class CopyingBlobs
    {
        private const string ContainerPrefix = "twitter-ingest-";
        private const string IngestContainerName = "twitter-ingest";
        private const string DropContainerName = "twitter-drops";

        public static void getstorageaccount()
        {
            Console.WriteLine("test1 - getstorageaccount");
            //CreateNewContrainer().Wait();
            BlobActions().Wait();
        }

        public static async Task BlobActions()
        {
            // Get a reference to the storage account from the connection string.
            CloudStorageAccount storageAccount = Common.CreateStorageAccountFromConnectionString();

            // Create service client for credentialed access to the Blob service.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = null;
            ServiceProperties userServiceProperties = null;

            try
            {
                // Save the user's current service property/storage analytics settings.
                // This ensures that the sample does not permanently overwrite the user's analytics settings.
                // Note however that logging and metrics settings will be modified for the duration of the sample.
                userServiceProperties = await blobClient.GetServicePropertiesAsync();

                // Get a reference to a sample container.
                container = await CreateContainer(blobClient);

                // list all containers and blobs within the container
                //ListAllContainers(blobClient);

                //await ListBlobsFlatListing(blobClient.GetContainerReference(DropContainerName), 10);

                //Console.WriteLine("3. List Blobs in Container");
                //foreach (IListBlobItem blob in container.ListBlobs())
                //{
                //    // Blob type will be CloudBlockBlob, CloudPageBlob or CloudBlobDirectory
                //    // Use blob.GetType() and cast to appropriate type to gain access to properties specific to each type
                //    Console.WriteLine("- {0} (type: {1})", blob.Uri, blob.GetType());
                //}

                //// copying bolobs
                //CloudBlobContainer sourcecontrainer = null;
                //sourcecontrainer = blobClient.GetContainerReference(DropContainerName);

                //CloudBlobContainer targetcontrainer = null;
                //targetcontrainer = blobClient.GetContainerReference(IngestContainerName);

                //CloudBlockBlob source = sourcecontrainer.GetBlockBlobReference("CSTwit.csv");
                //CloudBlockBlob target = targetcontrainer.GetBlockBlobReference("123/CSTwit22222222222222.csv");

                //await target.StartCopyAsync(source);

                //string BlobName = "CSTwit.csv";
                //await CopyBlobs(blobClient, DropContainerName, IngestContainerName, BlobName);

                await CopyBlobsListing(blobClient, blobClient.GetContainerReference(DropContainerName));


            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
            finally
            {
                // Delete the sample container created by this session.
                //if (container != null)
                //{
                //    await container.DeleteIfExistsAsync();
                //}

                // The sample code deletes any containers it created if it runs completely. However, if you need to delete containers 
                // created by previous sessions (for example, if you interrupted the running code before it completed), 
                // you can uncomment and run the following line to delete them.
                // await DeleteContainersWithPrefix(blobClient, ContainerPrefix);

                // Return the service properties/storage analytics settings to their original values.
                await blobClient.SetServicePropertiesAsync(userServiceProperties);
            }
        }

        private static async Task CopyBlobs(CloudBlobClient blobClient, String SourceContainerName, String TargetContainerName, String BlobName)
        {
            Console.WriteLine("=================================================");
            Console.WriteLine("Copying Blobs");
            Console.WriteLine();
            String NewFolder = "";

            //DateTime dt = DateTime.Now;
            DateTime dt = DateTime.UtcNow;
            string roundedmiutes = "00";
            if (dt.Minute < 15 && dt.Minute >= 0)
            {
                roundedmiutes = "15";
            }
            if (dt.Minute < 30 && dt.Minute >= 15)
            {
                roundedmiutes = "30";
            }
            if (dt.Minute < 45 && dt.Minute >= 30)
            {
                roundedmiutes = "45";
            }
            if (dt.Minute < 60 && dt.Minute >= 45)
            {
                roundedmiutes = "00";
            }

            NewFolder = string.Format("{0:yyyy}", dt) + "/" + string.Format("{0:MM}", dt) + "/" + string.Format("{0:dd}", dt) + "/" + string.Format("{0:hh}", dt) + "/" + roundedmiutes + "/";

            CloudBlobContainer sourcecontrainer = null;
            sourcecontrainer = blobClient.GetContainerReference(SourceContainerName);

            CloudBlobContainer targetcontrainer = null;
            targetcontrainer = blobClient.GetContainerReference(TargetContainerName);

            CloudBlockBlob source = sourcecontrainer.GetBlockBlobReference(BlobName);
            CloudBlockBlob target = targetcontrainer.GetBlockBlobReference(NewFolder + BlobName);

            await target.StartCopyAsync(source);
        }

        private static async Task CopyBlobsListing(CloudBlobClient blobClient, CloudBlobContainer container)
        {
            // List blobs to the console window.
            Console.WriteLine("=================================================");
            Console.WriteLine("Copy blobs in segments (flat listing):");
            Console.WriteLine();

            int i = 0;
            BlobContinuationToken continuationToken = null;
            BlobResultSegment resultSegment = null;

            try
            {
                // Call ListBlobsSegmentedAsync and enumerate the result segment returned, while the continuation token is non-null.
                // When the continuation token is null, the last segment has been returned and execution can exit the loop.
                do
                {
                    // This overload allows control of the segment size. You can return all remaining results by passing null for the maxResults parameter, 
                    // or by calling a different overload.
                    // Note that requesting the blob's metadata as part of the listing operation 
                    // populates the metadata, so it's not necessary to call FetchAttributes() to read the metadata.
                    resultSegment = await container.ListBlobsSegmentedAsync(string.Empty, true, BlobListingDetails.Metadata, null, continuationToken, null, null);
                    if (resultSegment.Results.Count() > 0)
                    {
                        Console.WriteLine("Page {0}:", ++i);
                    }

                    foreach (var blobItem in resultSegment.Results)
                    {
                        Console.WriteLine("************************************");
                        Console.WriteLine(blobItem.Uri);

                        // A flat listing operation returns only blobs, not virtual directories.
                        // Write out blob properties and metadata.
                        if (blobItem is CloudBlob)
                        {
                            Console.WriteLine(blobItem.Uri.Segments.Last());
                            await CopyBlobs(blobClient, DropContainerName, IngestContainerName, blobItem.Uri.Segments.Last());
                        }
                    }

                    Console.WriteLine();

                    // Get the continuation token.
                    continuationToken = resultSegment.ContinuationToken;
                }
                while (continuationToken != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        private static async Task<CloudBlobContainer> CreateContainer(CloudBlobClient blobClient)
        {
            // Name sample container based on new GUID, to ensure uniqueness.
            //string containerName = ContainerPrefix + Guid.NewGuid();
            string containerName = IngestContainerName;

            // Get a reference to a sample container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            try
            {
                // Create the container if it does not already exist.
                await container.CreateIfNotExistsAsync();
                Console.WriteLine("Continer created: " + containerName);
            }
            catch (StorageException e)
            {
                // Ensure that the storage emulator is running if using emulator connection string.
                Console.WriteLine(e.Message);
                Console.WriteLine("If you are running with the default connection string, please make sure you have started the storage emulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            return container;
        }

        private static async Task CreateNewContrainer()
        {
            //string containerName = ContainerPrefix + Guid.NewGuid();
            string containerName = "twitter-ingest";

            // Retrieve storage account information from connection string
            CloudStorageAccount storageAccount = Common.CreateStorageAccountFromConnectionString();

            // Create a blob client for interacting with the blob service.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Create a container for organizing blobs within the storage account.
            Console.WriteLine("1. Creating Container");
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            try
            {
                // The call below will fail if the sample is configured to use the storage emulator in the connection string, but 
                // the emulator is not running.
                // Change the retry policy for this call so that if it fails, it fails quickly.
                BlobRequestOptions requestOptions = new BlobRequestOptions() { RetryPolicy = new NoRetry() };
                await container.CreateIfNotExistsAsync(requestOptions, null);
                Console.WriteLine("Container Created:" + containerName);
            }
            catch (StorageException)
            {
                Console.WriteLine("If you are running with the default connection string, please make sure you have started the storage emulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                Console.ReadLine();
                throw;
            }
        }


        private static async Task ListBlobsFlatListing(CloudBlobContainer container, int? segmentSize)
        {
            // List blobs to the console window.
            Console.WriteLine("=================================================");
            Console.WriteLine("List blobs in segments (flat listing):");
            Console.WriteLine();

            int i = 0;
            BlobContinuationToken continuationToken = null;
            BlobResultSegment resultSegment = null;

            try
            {
                // Call ListBlobsSegmentedAsync and enumerate the result segment returned, while the continuation token is non-null.
                // When the continuation token is null, the last segment has been returned and execution can exit the loop.
                do
                {
                    // This overload allows control of the segment size. You can return all remaining results by passing null for the maxResults parameter, 
                    // or by calling a different overload.
                    // Note that requesting the blob's metadata as part of the listing operation 
                    // populates the metadata, so it's not necessary to call FetchAttributes() to read the metadata.
                    resultSegment = await container.ListBlobsSegmentedAsync(string.Empty, true, BlobListingDetails.Metadata, segmentSize, continuationToken, null, null);
                    if (resultSegment.Results.Count() > 0)
                    {
                        Console.WriteLine("Page {0}:", ++i);
                    }

                    foreach (var blobItem in resultSegment.Results)
                    {
                        Console.WriteLine("************************************");
                        Console.WriteLine(blobItem.Uri);

                        // A flat listing operation returns only blobs, not virtual directories.
                        // Write out blob properties and metadata.
                        if (blobItem is CloudBlob)
                        {
                            PrintBlobPropertiesAndMetadata((CloudBlob)blobItem);
                        }
                    }

                    Console.WriteLine();

                    // Get the continuation token.
                    continuationToken = resultSegment.ContinuationToken;
                }
                while (continuationToken != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        private static async void ListAllContainers(CloudBlobClient blobClient)
        {
            // List all containers in this storage account.
            Console.WriteLine("************************************");
            Console.WriteLine("List all containers in account:");

            try
            {
                foreach (var container in blobClient.ListContainers())
                {
                    Console.WriteLine("\tContainer:" + container.Name);

                    await ListBlobsFlatListing(container, 10);
                }

                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        private static void PrintBlobPropertiesAndMetadata(CloudBlob blob)
        {
            // Write out properties that are common to all blob types.
            Console.WriteLine();
            Console.WriteLine("-----Blob Properties-----");
            Console.WriteLine("\t Name: {0}", blob.Name);
            Console.WriteLine("\t Container: {0}", blob.Container.Name);
            Console.WriteLine("\t BlobType: {0}", blob.Properties.BlobType);
            Console.WriteLine("\t IsSnapshot: {0}", blob.IsSnapshot);

            // If the blob is a snapshot, write out snapshot properties.
            if (blob.IsSnapshot)
            {
                Console.WriteLine("\t SnapshotTime: {0}", blob.SnapshotTime);
                Console.WriteLine("\t SnapshotQualifiedUri: {0}", blob.SnapshotQualifiedUri);
            }

            Console.WriteLine("\t LeaseState: {0}", blob.Properties.LeaseState);

            // If the blob has been leased, write out lease properties.
            if (blob.Properties.LeaseState != LeaseState.Available)
            {
                Console.WriteLine("\t LeaseDuration: {0}", blob.Properties.LeaseDuration);
                Console.WriteLine("\t LeaseStatus: {0}", blob.Properties.LeaseStatus);
            }

            Console.WriteLine("\t CacheControl: {0}", blob.Properties.CacheControl);
            Console.WriteLine("\t ContentDisposition: {0}", blob.Properties.ContentDisposition);
            Console.WriteLine("\t ContentEncoding: {0}", blob.Properties.ContentEncoding);
            Console.WriteLine("\t ContentLanguage: {0}", blob.Properties.ContentLanguage);
            Console.WriteLine("\t ContentMD5: {0}", blob.Properties.ContentMD5);
            Console.WriteLine("\t ContentType: {0}", blob.Properties.ContentType);
            Console.WriteLine("\t ETag: {0}", blob.Properties.ETag);
            Console.WriteLine("\t LastModified: {0}", blob.Properties.LastModified);
            Console.WriteLine("\t Length: {0}", blob.Properties.Length);

            // Write out properties specific to blob type.
            switch (blob.BlobType)
            {
                case BlobType.AppendBlob:
                    CloudAppendBlob appendBlob = blob as CloudAppendBlob;
                    Console.WriteLine("\t AppendBlobCommittedBlockCount: {0}", appendBlob.Properties.AppendBlobCommittedBlockCount);
                    Console.WriteLine("\t StreamWriteSizeInBytes: {0}", appendBlob.StreamWriteSizeInBytes);
                    break;
                case BlobType.BlockBlob:
                    CloudBlockBlob blockBlob = blob as CloudBlockBlob;
                    Console.WriteLine("\t StreamWriteSizeInBytes: {0}", blockBlob.StreamWriteSizeInBytes);
                    break;
                case BlobType.PageBlob:
                    CloudPageBlob pageBlob = blob as CloudPageBlob;
                    Console.WriteLine("\t PageBlobSequenceNumber: {0}", pageBlob.Properties.PageBlobSequenceNumber);
                    Console.WriteLine("\t StreamWriteSizeInBytes: {0}", pageBlob.StreamWriteSizeInBytes);
                    break;
                default:
                    break;
            }

            Console.WriteLine("\t StreamMinimumReadSizeInBytes: {0}", blob.StreamMinimumReadSizeInBytes);
            Console.WriteLine();

            // Enumerate the blob's metadata.
            Console.WriteLine("Blob metadata:");
            foreach (var metadataItem in blob.Metadata)
            {
                Console.WriteLine("\tKey: {0}", metadataItem.Key);
                Console.WriteLine("\tValue: {0}", metadataItem.Value);
            }

            Console.WriteLine();
        }
    }
}
