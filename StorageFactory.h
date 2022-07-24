#include "S3BackupStorage.h"

class BackupStorageFactory
{
public:
	BackupStorageFactory(string type, string clientId, string volumeId, string region)
	{
		if (type == "s3" || type == "glacier")
		{
			m_storage = new S3BackupStorage(clientId, volumeId, region);
		}
	}

	BackupStorage* GetStorage() { return m_storage; }

	~BackupStorageFactory()
	{
		if (m_storage)
		{
			delete m_storage;
			m_storage = NULL;
		}
	}
private:
	BackupStorage* m_storage;
};