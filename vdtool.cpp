#include "StorageFactory.h"
#include "BackupProcessor.h"
#include <aws/core/utils/json/JsonSerializer.h>

using namespace Aws::Utils::Json;

int main(int argc, char* argv[])
{
	ofstream out("log.txt");
	streambuf *coutbuf = std::cout.rdbuf();
	cout.rdbuf(out.rdbuf());

	InputParams params;
	ifstream ifs("input.json");
	string content((istreambuf_iterator<char>(ifs)), (istreambuf_iterator<char>()));

	JsonValue json(content);
	auto v = json.View();

	if (!v.ValueExists("serverName"))
	{
		cout << "serverName missed";
		return ERROR_CODE;
	}

	auto values = v.GetAllObjects();
	string vmName = values["vmName"].AsString();
	string serverName = values["serverName"].AsString();
	string thumbPrint = values["thumbPrint"].AsString();

	auto credentials = values["credentials"].GetAllObjects();
	string userName = credentials["userName"].AsString();
	string password = credentials["password"].AsString();

	params.cnxParams = { 0 };
	params.cnxParams.credType = VIXDISKLIB_CRED_UID;
	params.cnxParams.serverName = (char*)serverName.c_str();
	params.cnxParams.thumbPrint = (char*)thumbPrint.c_str();
	params.cnxParams.vmxSpec = (char*)vmName.c_str();
	params.cnxParams.creds.uid.userName = (char*)userName.c_str();
	params.cnxParams.creds.uid.password = (char*)password.c_str();
	params.cnxParams.port = values["port"].AsInteger();
	params.cfgFile = values["cfgFile"].AsString();
	params.libDir = values["libDir"].AsString();
	params.snapshotRef = values["snapshotRef"].AsString();
	params.transportModes = values["transportModes"].AsString();
	params.fullBackup = values["fullBackup"].AsBool();
	params.restore = values["restore"].AsBool();
	params.volumeSize = values["volumeSize"].AsInteger();

	params.vmdk = values["vmdk"].AsString();

	auto s3values = values["s3"].GetAllObjects();

	string clientId = s3values["clientId"].AsString();
	string volumeId = s3values["volumeId"].AsString();
	string backupId = s3values["backupId"].AsString();
	string restoreId = s3values["restoreId"].AsString();
	string region = s3values["region"].AsString();

	auto factory = new BackupStorageFactory("s3", clientId, volumeId, region);
	auto backupProcessor = new BackupProcessor(factory->GetStorage(), backupId);

	int result = 0;

	if (params.restore)
	{
		result = backupProcessor->RestoreData(params, volumeId, restoreId);
	}
	else
	{
		result = backupProcessor->BackupData(params);
	}

	delete backupProcessor;
	delete factory;

	cout.rdbuf(coutbuf);

	return result;
}