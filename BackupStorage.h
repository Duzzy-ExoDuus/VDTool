#ifndef BACKUPSTORAGE_H
#define BACKUPSTORAGE_H

#include "CommonTypes.h"

using namespace std;

//abstract class, inherit from this class to implement custom backup storage
class BackupStorage
{
public:
	BackupStorage() {};

	BackupStorage(string clientId, string volumeId, string region) :
		m_clientId(clientId),
		m_volumeId(volumeId),
		m_region(region)
	{
	}

	virtual int GetFreeBufferOffsetIndex() = 0;

	virtual void UploadBackupSectorDataAsync(string backupId, string item, string key, const char* bufferOffset, int bufferOffsetIndex, size_t bufferSize) = 0;

	virtual void WaitForAllUploadTasksToComplete() = 0;

	virtual void UploadBackupMetaData(string backupId, BackupMetaData &metadata) = 0;

	virtual VolumeMetaData GetVolumeMetaData(string volumeId) = 0;

	virtual BackupMetaData GetBackupMetaData(string backupId) = 0;

	virtual void UploadRestoreTaskMetaData(const RestoreTaskMetaData &metadata) = 0;

	virtual RestoreTaskMetaData GetRestoreTaskMetaData(string restoreId) = 0;

	virtual int GetBackupBlockData(string backupId, int partId, string key, const vector<int>& indices, char* buffer) = 0;

	virtual int ListObjects(string backupId, int partId, vector<int>& objects) = 0;

protected:
	string m_clientId;
	string m_volumeId;
	string m_region;
};

#endif