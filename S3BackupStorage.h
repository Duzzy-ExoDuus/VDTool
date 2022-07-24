#include <fstream>
#include <aws/core/Aws.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include "core/membuf.h"
#include "core/thread_safe_queue.h"
#include "BackupStorage.h"

using namespace Aws;
using namespace S3;
using namespace S3::Model;

class S3BackupStorage : public BackupStorage
{
public:
	S3BackupStorage(string clientId, string volumeId, string region);

	S3BackupStorage() = delete;
	S3BackupStorage(const S3BackupStorage&) = delete;
	S3BackupStorage& operator =(const S3BackupStorage&) = delete;
	S3BackupStorage(S3BackupStorage&&) = delete;
	S3BackupStorage& operator =(S3BackupStorage&&) = delete;

	~S3BackupStorage();

	int GetFreeBufferOffsetIndex() override;

	void UploadBackupSectorDataAsync(string backupId, string item, string key, const char* bufferOffset, int bufferOffsetIndex, size_t bufferSize) override;

	void WaitForAllUploadTasksToComplete() override;

	void UploadBackupMetaData(string backupId, BackupMetaData &metadata) override;

	VolumeMetaData GetVolumeMetaData(string volumeId) override;

	BackupMetaData GetBackupMetaData(string backupId) override;

	void UploadRestoreTaskMetaData(const RestoreTaskMetaData &metadata) override;

	RestoreTaskMetaData GetRestoreTaskMetaData(string restoreId) override;

	int GetBackupBlockData(string backupId, int partId, string key, const vector<int>& indices, char* buffer) override;

	int ListObjects(string backupId, int partId, vector<int>& objects) override;

private:
	string GetVolumeBucket() const;

	long m_connectTimeoutMs;
	long m_requestTimeoutMs;

	SDKOptions m_options;

	S3Client *m_s3Client;
};