#include "BackupStorage.h"
#include <aws/core/utils/json/JsonSerializer.h>
#include "vixDiskLib.h"
#include "vixMntapi.h"

using namespace std;

#define ERROR_CODE 1

struct ChangedDiskArea
{
	UINT64 length;
	UINT64 start;
};

class BackupProcessor
{
public:
	BackupProcessor(BackupStorage* backupStorage, string backupId);

	BackupProcessor(const BackupProcessor&) = delete;
	BackupProcessor& operator = (const BackupProcessor&) = delete;
	BackupProcessor(BackupProcessor&&) = delete;
	BackupProcessor& operator = (BackupProcessor&&) = delete;

	int BackupData(InputParams& params);
	int RestoreData(InputParams& params, string volumeId, string restoreId);

private:
	int QueryAllocatedBlocks(VixDiskLibHandle& handle);
	int ReadChangedDiskAreas();

	VixError BackupTaskWithError(VixError vixError);
	VixError RestoreTaskWithError(VixError vixError);
	VixError ProcessWriteResult(VixError vixError, char* buffer, VixDiskLibHandle handle, VixDiskLibConnection connection);

	BackupStorage* m_backupStorage;

	string m_device;
	string m_backupId;

	vector<ChangedDiskArea> m_changedDiskAreas;
};