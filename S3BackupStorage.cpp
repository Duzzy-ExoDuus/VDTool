#include "S3BackupStorage.h"

atomic_int upload_tasks_running(0);
SafeQueue<int> upload_queue;

void PutObjectResultHandler(const S3Client* client, const PutObjectRequest& request, const PutObjectOutcome& outcome, const shared_ptr<const Client::AsyncCallerContext>& context)
{
	if (!outcome.IsSuccess())
	{
		cout << "Error: " << outcome.GetError().GetExceptionName() << " - " << outcome.GetError().GetMessage() << endl;
	}

	int index = stoi(context->GetUUID());

	upload_queue.enqueue(index);

	upload_tasks_running--;
}

int GetDataBlock(string bucket, string region, string key, const char* item, char* dstBuffer)
{
	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = region.c_str();
	S3Client s3_client(config);

	GetObjectRequest request;
	request.WithBucket(bucket.c_str()).WithKey(item);

	if (!key.empty())
	{
		auto keyEncoded = Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::ByteBuffer((unsigned char*)key.c_str(), key.length()));
		auto md5Encoded = Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateMD5(Aws::String(key.c_str())));

		request.SetSSECustomerAlgorithm("AES256");
		request.SetSSECustomerKey(keyEncoded);
		request.SetSSECustomerKeyMD5(md5Encoded);
	}

	auto outcome = s3_client.GetObject(request);
	uint32_t contentLength = 0;

	if (outcome.IsSuccess())
	{
		contentLength = (uint32_t)outcome.GetResult().GetContentLength();

		std::streambuf* cbuf = outcome.GetResult().GetBody().rdbuf();
		cbuf->sgetn(dstBuffer, contentLength);
	}
	else
	{
		cout << "Error: " << outcome.GetError().GetExceptionName() << " - " << outcome.GetError().GetMessage() << endl;
	}

	return contentLength;
}

S3BackupStorage::S3BackupStorage(string clientId,
								 string volumeId,
								 string region)
{
	m_connectTimeoutMs = 30000;
	m_requestTimeoutMs = 600000;
	m_clientId = clientId;
	m_volumeId = volumeId;
	m_region = region;

	InitAPI(m_options);

	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = m_region.c_str();
	config.connectTimeoutMs = m_connectTimeoutMs;
	config.requestTimeoutMs = m_requestTimeoutMs;
	config.executor = MakeShared<Utils::Threading::PooledThreadExecutor>("PooledThreadExecutor", 20);

	m_s3Client = new S3Client(config);

	for (int i = 0; i < UploadBatchSize; i++)
	{
		upload_queue.enqueue(i);
	}
}

S3BackupStorage::~S3BackupStorage()
{
	if (m_s3Client != NULL)
	{
		delete m_s3Client;
	}

	ShutdownAPI(m_options);
}

VolumeMetaData S3BackupStorage::GetVolumeMetaData(string volumeId)
{
	VolumeMetaData metadata;

	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = m_region.c_str();
	config.connectTimeoutMs = m_connectTimeoutMs;
	config.requestTimeoutMs = m_requestTimeoutMs;
	S3Client s3_client(config);

	string bucket = GetVolumeBucket() + "/metadata";

	GetObjectRequest request;
	request.WithBucket(bucket.c_str()).WithKey("metadata");

	auto outcome = s3_client.GetObject(request);

	if (outcome.IsSuccess())
	{
		auto size = outcome.GetResult().GetContentLength();
		char* buffer = (char*)malloc(size);
		std::streambuf* cbuf = outcome.GetResult().GetBody().rdbuf();
		cbuf->sgetn(buffer, size);

		uint32_t num = *(uint32_t*)buffer;
		uint32_t pos = sizeof(uint32_t);

		for (uint32_t i = 0; i < num; i++)
		{
			string backupId = string(buffer + pos, BACKUP_UUID_SIZE * sizeof(char));
			metadata.backupIds.push_back(backupId);
			pos += BACKUP_UUID_SIZE * sizeof(char);
		}

		free(buffer);
	}
	else
	{
		cout << "Error: "
			<< outcome.GetError().GetExceptionName() << " - "
			<< outcome.GetError().GetMessage() << endl;
	}

	return metadata;
}

BackupMetaData S3BackupStorage::GetBackupMetaData(string backupId)
{
	BackupMetaData metadata;

	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = m_region.c_str();
	config.connectTimeoutMs = m_connectTimeoutMs;
	config.requestTimeoutMs = m_requestTimeoutMs;
	S3Client s3_client(config);

	string bucket = GetVolumeBucket() + "/backups/" + backupId + "/metadata";

	GetObjectRequest request;
	request.WithBucket(bucket.c_str()).WithKey("metadata");

	auto outcome = s3_client.GetObject(request);

	if (outcome.IsSuccess())
	{
		auto size = outcome.GetResult().GetContentLength();
		char* buffer = (char*)malloc(size);
		std::streambuf* cbuf = outcome.GetResult().GetBody().rdbuf();
		cbuf->sgetn(buffer, size);

		metadata.status = (BackupStatus)(*(uint32_t*)buffer);
		uint32_t pos = sizeof(uint32_t);
		uint32_t num = *(uint32_t*)(buffer + pos);
		pos += sizeof(uint32_t);

		uint32_t emptyBlocks = *(uint32_t*)(buffer + pos);
		pos += sizeof(uint32_t);

		uint32_t encryptionKeyLength = *(uint32_t*)(buffer + pos);
		pos += sizeof(uint32_t);

		metadata.encryptionKey = string(buffer + pos, encryptionKeyLength * sizeof(char));
		pos += encryptionKeyLength * sizeof(char);

		for (uint32_t i = 0; i < num; i++)
		{
			uint32_t* keyPtr = (uint32_t*)(buffer + pos);
			uint32_t key = *keyPtr;

			uint64_t* valuePtr = (uint64_t*)(buffer + pos + sizeof(uint32_t));
			uint64_t value = *valuePtr;

			metadata.blockHashTable[key] = value;

			pos += (sizeof(uint32_t) + sizeof(uint64_t));
		}

		for (uint32_t j = 0; j < emptyBlocks; j++)
		{
			uint32_t* ptr = (uint32_t*)(buffer + pos);
			uint32_t index = *ptr;

			metadata.emptyBlocks.push_back(index);

			pos += sizeof(uint32_t);
		}

		free(buffer);
	}
	else
	{
		cout << "Error: "
			<< outcome.GetError().GetExceptionName() << " - "
			<< outcome.GetError().GetMessage() << endl;
	}

	return metadata;
}

string S3BackupStorage::GetVolumeBucket() const
{
	return m_clientId + "/" + m_volumeId;
}

int S3BackupStorage::GetBackupBlockData(string backupId, int partId, string key, const vector<int>& indices, char* buffer)
{
	int size = 0;

	for (int i = 0; i < indices.size(); i++)
	{
		int index = indices[i];
		string item = to_string(index + 1);

		string bucket = GetVolumeBucket() + "/backups/" + backupId + "/blockdata/" + to_string(partId + 1);
		size += GetDataBlock(bucket, m_region, key, item.c_str(), buffer);
	}

	return size;
}

void S3BackupStorage::WaitForAllUploadTasksToComplete()
{
	while (true)
	{
		if (upload_tasks_running == 0)
		{
			break;
		}
		else
		{
			this_thread::sleep_for(1s);
		}
	}
}

int S3BackupStorage::GetFreeBufferOffsetIndex()
{
	int index = upload_queue.dequeue();

	return index;
}

void S3BackupStorage::UploadBackupSectorDataAsync(string backupId, string item, string key, const char* bufferOffset, int bufferOffsetIndex, size_t bufferSize)
{
	char *ptr = (char*)bufferOffset;
	const char *cstr = item.c_str();
	streambuf *buf = new membuf(ptr, ptr + bufferSize);
	string bucket = GetVolumeBucket() + "/backups/" + backupId + "/blockdata/";
		
	auto objectStream = MakeShared<IOStream>("BlockUpload", buf);
	
	PutObjectRequest request;
	request.WithBucket(bucket.c_str()).WithKey(cstr).WithTagging("calamu");
	request.SetBody(objectStream);
	request.SetContentLength(bufferSize);

	if (!key.empty())
	{
		auto keyEncoded = Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::ByteBuffer((unsigned char*)key.c_str(), key.length()));
		auto md5Encoded = Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateMD5(Aws::String(key.c_str())));

		request.SetSSECustomerAlgorithm("AES256");
		request.SetSSECustomerKey(keyEncoded);
		request.SetSSECustomerKeyMD5(md5Encoded);
	}

	shared_ptr<Client::AsyncCallerContext> context = MakeShared<Client::AsyncCallerContext>("PutObjectAllocationTag");
	context->SetUUID(to_string(bufferOffsetIndex));

	upload_tasks_running++;

	m_s3Client->PutObjectAsync(request, PutObjectResultHandler, context);
}

void S3BackupStorage::UploadBackupMetaData(string backupId, BackupMetaData &metadata)
{
	size_t size = 3 * sizeof(int) +
		(sizeof(uint32_t) + sizeof(char) * metadata.encryptionKey.length()) +
		(sizeof(uint32_t) + sizeof(uint64_t)) * metadata.blockHashTable.size() +
		(sizeof(uint32_t) * metadata.emptyBlocks.size());

	char* buffer = (char*)malloc(size);

	long pos = 0;

	uint32_t status = (uint32_t)metadata.status;
	memcpy(buffer, (void*)&status, sizeof(uint32_t));
	pos += sizeof(uint32_t);

	uint32_t num = (uint32_t)metadata.blockHashTable.size();
	memcpy(buffer + pos, (void*)&num, sizeof(uint32_t));
	pos += sizeof(uint32_t);

	uint32_t emptyBlocks = (uint32_t)metadata.emptyBlocks.size();
	memcpy(buffer + pos, (void*)&emptyBlocks, sizeof(uint32_t));
	pos += sizeof(uint32_t);

	uint32_t keyLength = (uint32_t)metadata.encryptionKey.length();
	memcpy(buffer + pos, (void*)&keyLength, sizeof(uint32_t));
	pos += sizeof(uint32_t);

	if (keyLength > 0)
	{
		const char* ptr = metadata.encryptionKey.c_str();
		memcpy(buffer + pos, (void*)ptr, keyLength * sizeof(char));
		pos += keyLength * sizeof(char);
	}

	for (auto iter = metadata.blockHashTable.begin(); iter != metadata.blockHashTable.end(); iter++)
	{
		memcpy(buffer + pos, (void*)&(iter->first), sizeof(uint32_t));
		memcpy(buffer + pos + sizeof(uint32_t), (void*)&(iter->second), sizeof(uint64_t));

		pos += (sizeof(uint32_t) + sizeof(uint64_t));
	}

	for (size_t i = 0; i < metadata.emptyBlocks.size(); i++)
	{
		uint32_t index = metadata.emptyBlocks[i];
		memcpy(buffer + pos, (void*)&index, sizeof(uint32_t));

		pos += sizeof(uint32_t);
	}

	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = m_region.c_str();
	config.connectTimeoutMs = m_connectTimeoutMs;
	config.requestTimeoutMs = m_requestTimeoutMs;
	S3Client s3_client(config);

	streambuf *buf = new membuf(buffer, buffer + size);
	auto objectStream = MakeShared<IOStream>("BlockUpload", buf);

	string bucket = GetVolumeBucket() + "/backups/" + backupId + "/metadata";

	PutObjectRequest request;
	request.WithBucket(bucket.c_str()).WithKey("metadata");
	request.SetBody(objectStream);
	request.SetContentLength(size);

	auto outcome = s3_client.PutObject(request);

	if (!outcome.IsSuccess())
	{
		cout << "Error: "
			<< outcome.GetError().GetExceptionName() << " - "
			<< outcome.GetError().GetMessage() << endl;
	}

	free(buffer);
}

RestoreTaskMetaData S3BackupStorage::GetRestoreTaskMetaData(string restoreId)
{
	RestoreTaskMetaData metadata;
	metadata.restoreId = restoreId;

	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = m_region.c_str();
	config.connectTimeoutMs = m_connectTimeoutMs;
	config.requestTimeoutMs = m_requestTimeoutMs;
	S3Client s3_client(config);

	string bucket = GetVolumeBucket() + "/restore";

	GetObjectRequest request;
	request.WithBucket(bucket.c_str()).WithKey(restoreId.c_str());

	auto outcome = s3_client.GetObject(request);

	if (outcome.IsSuccess())
	{
		auto size = outcome.GetResult().GetContentLength();
		char* buffer = (char*)malloc(size);
		std::streambuf* cbuf = outcome.GetResult().GetBody().rdbuf();
		cbuf->sgetn(buffer, size);

		metadata.status = (RestoreStatus)(*(uint32_t*)buffer);
		uint32_t pos = sizeof(uint32_t);

		uint32_t encryptionKeyLength = *(uint32_t*)(buffer + pos);
		pos += sizeof(uint32_t);

		if (encryptionKeyLength > 0)
		{
			metadata.encryptionKey = string(buffer + pos, encryptionKeyLength * sizeof(char));
			pos += encryptionKeyLength * sizeof(char);
		}

		free(buffer);
	}
	else
	{
		cout << "Error: "
			<< outcome.GetError().GetExceptionName() << " - "
			<< outcome.GetError().GetMessage() << endl;
	}

	return metadata;
}

void S3BackupStorage::UploadRestoreTaskMetaData(const RestoreTaskMetaData &metadata)
{
	size_t size = 2 * sizeof(uint32_t) + sizeof(char) * metadata.encryptionKey.length();
	char* buffer = (char*)malloc(size);
	long pos = 0;

	uint32_t status = (uint32_t)metadata.status;
	memcpy(buffer, (void*)&status, sizeof(uint32_t));
	pos += sizeof(uint32_t);

	uint32_t keyLength = (uint32_t)metadata.encryptionKey.length();
	memcpy(buffer + pos, (void*)&keyLength, sizeof(uint32_t));
	pos += sizeof(uint32_t);

	if (keyLength > 0)
	{
		const char* ptr = metadata.encryptionKey.c_str();
		memcpy(buffer + pos, (void*)ptr, keyLength * sizeof(char));
		pos += keyLength * sizeof(char);
	}

	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = m_region.c_str();
	config.connectTimeoutMs = m_connectTimeoutMs;
	config.requestTimeoutMs = m_requestTimeoutMs;
	S3Client s3_client(config);

	streambuf *buf = new membuf(buffer, buffer + size);
	auto objectStream = MakeShared<IOStream>("BlockUpload", buf);

	string bucket = GetVolumeBucket() + "/restore";

	PutObjectRequest request;
	request.WithBucket(bucket.c_str()).WithKey(metadata.restoreId.c_str());
	request.SetBody(objectStream);
	request.SetContentLength(size);

	auto outcome = s3_client.PutObject(request);

	if (!outcome.IsSuccess())
	{
		cout << "Error: "
			<< outcome.GetError().GetExceptionName() << " - "
			<< outcome.GetError().GetMessage() << endl;
	}

	free(buffer);
}

int S3BackupStorage::ListObjects(string backupId, int partId, vector<int>& objects)
{
	int result = 0;
	Client::ClientConfiguration config;
	config.scheme = Http::Scheme::HTTPS;
	config.region = m_region.c_str();
	config.connectTimeoutMs = m_connectTimeoutMs;
	config.requestTimeoutMs = m_requestTimeoutMs;
	S3Client s3_client(config);

	string bucket = m_clientId;
	string prefix = m_volumeId + "/backups/" + backupId + "/blockdata/" + std::to_string(partId + 1) + "/";
	ListObjectsRequest request;
	request.WithBucket(bucket.c_str()).WithPrefix(prefix.c_str()).WithDelimiter("/");

	ListObjectsOutcome outcome;

	do
	{
		outcome = s3_client.ListObjects(request);

		if (outcome.IsSuccess())
		{
			auto marker = outcome.GetResult().GetNextMarker();

			if (!marker.empty())
			{
				request.SetMarker(marker.c_str());
			}

			auto objects_list = outcome.GetResult().GetContents();

			for (auto iter = objects_list.begin(); iter != objects_list.end(); iter++)
			{
				if (iter->GetSize() > 0)
				{
					string key = iter->GetKey().c_str();
					size_t pos = key.find(prefix);

					if (pos != string::npos)
					{
						key.erase(0, pos + prefix.length());
					}

					auto object = atoi(key.c_str()) - 1;

					if (std::find(objects.begin(), objects.end(), object) == objects.end())
					{
						objects.push_back(object);
					}
				}
			}
		}
		else
		{
			cout << "Error: " << outcome.GetError().GetExceptionName() << " - " << outcome.GetError().GetMessage() << endl;
			return 1;
		}
	}
	while (outcome.GetResult().GetIsTruncated());

	return result;
}
