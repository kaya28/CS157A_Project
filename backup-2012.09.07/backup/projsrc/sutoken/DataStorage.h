#ifndef _DATASTORE_H
#define _DATASTORE_H

#pragma once
#import "msado15.dll" rename("EOF", "EndOfFile") //raw_interfaces_only	
#include <atlbase.h>
#include <AtlConv.h>
#include <atlcomcli.h>

class CDataStorage
{
	string m_DSN;
	ADODB::_ConnectionPtr m_AdoConnection;

public:
	CDataStorage(void);
	~CDataStorage(void);

	// Initiate DBMS connection
	bool OpenConnection(string sDSN, short iTimeOut=30, long eCursorLocation=3, long eConnectMode=3);

	// Execute any SQL statement (including stored procedure) without returning result list.
	bool ExecuteSQL(string & sSQL, short pTimeout=30);

	// Execute any SQL statement and return result list in the recordset.
	bool ExecuteSQL_IntoRecordset(ADODB::_RecordsetPtr& spRsNew, string sSQL,  long lMaxRecords=-1, long eCursorType=2, long eLockType=4, bool bDisconnect=true);
	
	// Execute a SQL stored procedure that takes one parameter and return result list in the recordset.
	bool ExecuteSP_IntoRecordset(ADODB::_RecordsetPtr& spRsNew, string sStoredProc,  string sParam);

	// Close DBMS connection
	bool CloseConnection();

	// Insert new URLPage entry.
	bool sp_InsertURLPage(string pURL, long pPage, long pWordCount);
	
	// Update properties of a concept
	bool sp_UpdateConcept(CToken& pToken, long pTupleIndex);
	
	// troubleshooting functions
	bool TestConnection(void);
};

#endif
