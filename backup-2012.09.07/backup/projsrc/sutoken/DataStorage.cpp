#include "StdAfx.h"
#include "DataStorage.h"


CDataStorage::CDataStorage(void)
{
}


CDataStorage::~CDataStorage(void)
{
}


bool CDataStorage::OpenConnection(string sDSN, short iTimeOut, long eCursorLocation, long eConnectMode)
{
	bool bRtn=false;
	USES_CONVERSION;

	try {
		HRESULT hr = m_AdoConnection.CreateInstance(__uuidof(ADODB::Connection));
		if (SUCCEEDED(hr))
		{
			m_DSN = sDSN;
			_bstr_t aBSTR = _bstr_t(sDSN.c_str());// T2OLE ((LPTSTR)sDSN.c_str() );

			m_AdoConnection->put_ConnectionString(aBSTR);		
			m_AdoConnection->put_CommandTimeout( iTimeOut );
			m_AdoConnection->put_CursorLocation( (ADODB::CursorLocationEnum )eCursorLocation );
			m_AdoConnection->put_Mode( (ADODB::ConnectModeEnum )eConnectMode );
		
			//OutputDebugString(aBSTR);
			hr = m_AdoConnection->Open(aBSTR,_bstr_t(""),_bstr_t(""),ADODB::adOptionUnspecified);
		
			if( hr == S_OK )
				bRtn = true;
		}
	}
	catch ( _com_error ex)
	{
		printf("*** Exception in OpenConnection()\n");
		printf( ex.Description());
	}

	return bRtn;
}

bool CDataStorage::CloseConnection()
{
	if( m_AdoConnection != NULL )
	{
		long connState;
		m_AdoConnection->get_State( &connState );
		if( connState == ADODB::adStateOpen )
			m_AdoConnection->Close();
	}

	m_AdoConnection=NULL;

	return true;
}

bool CDataStorage::sp_UpdateConcept(CToken& pToken, long pTupleIndex)
{
	bool bRtn = false;
	USES_CONVERSION;

	_variant_t  vtEmpty (DISP_E_PARAMNOTFOUND, VT_ERROR);
	_variant_t  vtEmpty2(DISP_E_PARAMNOTFOUND, VT_ERROR);
	ADODB::_CommandPtr     Cmd1;
	ADODB::_ParameterPtr	dbParamID, dbParamTokens, dbParamTokensOrig, dbParamTokenCount, dbParamFreq, 
							dbParamDocFreq, dbParamPerm, dbParamFirstPos, dbParamDistance;
	ADODB::_RecordsetPtr   Rs1;

	// Trap any error/exception.
	try
	{
		// Create Command Object.
       	Cmd1.CreateInstance( __uuidof( ADODB::Command ) );
		Cmd1->ActiveConnection = m_AdoConnection;
		Cmd1->CommandText = _bstr_t(L"exec sp_UpdateConcept ?, ?, ?, ?, ?, ?, ?, ?, ?");

		// Create Parameter Object.

		// ID
		dbParamID = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1,	 _variant_t( pToken.DBID() ) );
		//dbParamID->Value = _variant_t( pToken.DBID() );
		Cmd1->Parameters->Append( dbParamID );

		// Tokens
		dbParamTokens = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adVarChar, ADODB::adParamInput, -1, _variant_t( pToken.getToken() ) );
		//dbParamTokens->Value = _variant_t( pToken.getToken() );
		Cmd1->Parameters->Append( dbParamTokens );

		// TokensOrig
		dbParamTokensOrig = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adVarChar, ADODB::adParamInput, -1, _variant_t( pToken.getTokenOrig() ) );
		//dbParamTokens->Value = _variant_t( pToken.getTokenOrig() );
		Cmd1->Parameters->Append( dbParamTokensOrig );

		// TokenCount
		dbParamTokenCount = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1, _variant_t( pTupleIndex ) );
		//dbParamTokenCount->Value = _variant_t( pTupleIndex );
		Cmd1->Parameters->Append( dbParamTokenCount );

		// Frequency
		dbParamFreq = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1, _variant_t( pToken.Freq() ) );
		//dbParamFreq->Value = _variant_t( pToken.Freq() );
		Cmd1->Parameters->Append( dbParamFreq );

		// DocFrequency
		dbParamDocFreq = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1, _variant_t( pToken.DocFreq() ) );
		//dbParamDocFreq->Value = _variant_t( pToken.DocFreq() );
		Cmd1->Parameters->Append( dbParamDocFreq );

		// FirstPos
		dbParamFirstPos = Cmd1->CreateParameter(_bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1, _variant_t( pToken.FirstPos() ) );
		//dbParamFirstPos->Value = _variant_t( pToken.FirstPos() );
		Cmd1->Parameters->Append( dbParamFirstPos );

		// Distance
		dbParamDistance = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1, _variant_t( pToken.Distance() ) );
		//dbParamDistance->Value = _variant_t( pToken.LastPos() );
		Cmd1->Parameters->Append( dbParamDistance );

		// Perm
		dbParamPerm = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1, _variant_t( pToken.Perm() ) );
		//dbParamPerm->Value = _variant_t( pToken.Perm() );
		Cmd1->Parameters->Append( dbParamPerm );

		// Open Recordset Object.
		Rs1 = Cmd1->Execute(&vtEmpty, &vtEmpty2, ADODB::adCmdText );

		if (Rs1 != NULL)
			bRtn = true;

	}
	catch(...)
	{
		printf("*** sp_UpdateConcept EXCEPTION: %s\n", pToken.Token().c_str());
	}

	return bRtn;
}

bool CDataStorage::sp_InsertURLPage(string pURL, long pPageID, long pWordCount)
{
	bool bRtn = false;
	USES_CONVERSION;

	_variant_t  vtEmpty (DISP_E_PARAMNOTFOUND, VT_ERROR);
	_variant_t  vtEmpty2(DISP_E_PARAMNOTFOUND, VT_ERROR);
	ADODB::_CommandPtr     Cmd1;
	ADODB::_ParameterPtr   dbParamURL, dbParamID, dbParamWordCount;
	ADODB::_RecordsetPtr   Rs1;

	// Trap any error/exception.
	try
	{
		// Create Command Object.
		Cmd1.CreateInstance( __uuidof( ADODB::Command ) );
		Cmd1->ActiveConnection = m_AdoConnection;
		Cmd1->CommandText = _bstr_t(L"exec sp_InsertURLPage ?, ?, ?");

		// Create Parameter Object.
		dbParamID = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1,	 _variant_t( pPageID ) );
		//dbParamID->Value = _variant_t( pPageID);
		Cmd1->Parameters->Append( dbParamID );

		dbParamURL = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adVarChar, ADODB::adParamInput, -1, _variant_t( pURL.c_str() ) );
		//dbParamURL->Value = _variant_t( pURL.c_str() );
		Cmd1->Parameters->Append( dbParamURL );

		dbParamWordCount = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adInteger, ADODB::adParamInput, -1, _variant_t( pWordCount ) );
		//dbParamWordCount->Value = _variant_t( pWordCount);
		Cmd1->Parameters->Append( dbParamWordCount );

		// Open Recordset Object.
		Rs1 = Cmd1->Execute(&vtEmpty, &vtEmpty2, ADODB::adCmdText );

		bRtn = true;

	}
	catch(...)
	{
		printf("*** sp_InsertURLPage EXCEPTION: %s\n", pURL.c_str());
	}

	return bRtn;
}

// Execute stored proc 'sStoredProc' with one parameter 'sParam'
// For example, this is used for sProcCmd='sp_IsURLScanned' with sParam='C:/CS267/Data/inputfiles/files/doc4.txt'
bool CDataStorage::ExecuteSP_IntoRecordset(ADODB::_RecordsetPtr& spRsNew, string sStoredProc,  string sParam)
{
	bool bRtn = false;
	USES_CONVERSION;

	_variant_t  vtEmpty (DISP_E_PARAMNOTFOUND, VT_ERROR);
	_variant_t  vtEmpty2(DISP_E_PARAMNOTFOUND, VT_ERROR);
	ADODB::_CommandPtr     Cmd1;
	ADODB::_ParameterPtr   Param1;
	ADODB::_RecordsetPtr   Rs1;

	// Trap any error/exception.
	try
	{
		// Create Command Object.
		Cmd1.CreateInstance( __uuidof( ADODB::Command ) );
		Cmd1->ActiveConnection = m_AdoConnection;
		Cmd1->CommandText = _bstr_t(sStoredProc.c_str());

		// Create Parameter Object.
		Param1 = Cmd1->CreateParameter( _bstr_t(L""), ADODB::adVarChar, ADODB::adParamInput, -1, _variant_t( sParam.c_str()) );
		//Param1->Value = _variant_t( sParam.c_str());
		Cmd1->Parameters->Append( Param1 );

		// Open Recordset Object.
		Rs1 = Cmd1->Execute( &vtEmpty, &vtEmpty2, ADODB::adCmdText );
		if (S_OK == Rs1->QueryInterface(Rs1.GetIID(),(void**)&spRsNew ))
			bRtn = true;

	}
	catch(...)
	{
		printf("*** ExecuteSP_IntoRecordset EXCEPTION\n");
	}

	return bRtn;
}

bool CDataStorage::ExecuteSQL_IntoRecordset(ADODB::_RecordsetPtr& spRsNew, string sSQL,  long lMaxRecords, long eCursorType, long eLockType, bool bDisconnect)
{
	bool bRtn = false;
	USES_CONVERSION;

	try
	{
		ADODB::_RecordsetPtr loRsNew;
		HRESULT hr = loRsNew.CreateInstance(__uuidof(ADODB::Recordset));
		if (SUCCEEDED(hr))
		{
			//set rs parameters
			loRsNew->put_CacheSize( lMaxRecords > 0 ? lMaxRecords : 1 );
			loRsNew->put_MaxRecords( lMaxRecords > 0 ? lMaxRecords : 0 );
			loRsNew->put_CursorType( (ADODB::CursorTypeEnum)eCursorType );
			loRsNew->put_LockType( (ADODB::LockTypeEnum)eLockType );
			//open the rs
			if( m_AdoConnection != NULL )
			{
				// BSTR sSQL string to variant
				CComVariant vSQL( CComBSTR(sSQL.c_str()) );
				CComVariant voAdoConn( m_AdoConnection.GetInterfacePtr() );
				loRsNew->Open(vSQL, voAdoConn, ( (ADODB::CursorTypeEnum)eCursorType ), ( (ADODB::LockTypeEnum)eLockType ), ADODB::adCmdText);
			}
        
			//If disconnection was requested disconnect the rs
			if( bDisconnect )
				loRsNew->put_ActiveConnection( vtMissing );

			hr = loRsNew->QueryInterface(loRsNew.GetIID(),(void**)&spRsNew );

			if( hr == S_OK )
				bRtn = true;
		}
		
	}
	catch(_com_error &e)
	{
		//Handle errors
		char ErrorStr[1000];
		_bstr_t bstrSource(e.Source());
		_bstr_t bstrDescription(e.Description());

		sprintf_s(ErrorStr, 1000, "ExecuteSQL_IntoRecordset Error\n\tCode = %08lx\n\tCode meaning = %s\n\tSource = %s\n\tDescription = %s\n",
			e.Error(), e.ErrorMessage(), (LPCSTR)bstrSource, (LPCSTR)bstrDescription );
		
		printf(ErrorStr);
	}

	return bRtn;
}

bool CDataStorage::ExecuteSQL(string & sSQL, short pTimeout)
{
	bool bRtn = false;
	HRESULT hr = E_FAIL;

	try
	{
		ADODB::_RecordsetPtr spRs = NULL;
		m_AdoConnection->put_CommandTimeout( pTimeout );
		spRs = m_AdoConnection->Execute(_bstr_t(sSQL.c_str()),&vtMissing, -1);

		if( spRs != NULL )
			bRtn = true;
		//spRs->Close();
	}
	catch(_com_error &e)
	{
		//Handle errors
		char ErrorStr[1000];
		_bstr_t bstrSource(e.Source());
		_bstr_t bstrDescription(e.Description());

		sprintf_s(ErrorStr, 1000, "ExecuteSQL Error\n\tCode = %08lx\n\tCode meaning = %s\n\tSource = %s\n\tDescription = %s\n",
			e.Error(), e.ErrorMessage(), (LPCSTR)bstrSource, (LPCSTR)bstrDescription );
		printf(ErrorStr);
	}
	catch(...)
	{
		printf("*** CDataStorage::ExecuteSQL EXCEPTION\n");
	}
   	return bRtn;
}



bool CDataStorage::TestConnection(void)
{
	bool bRtn = OpenConnection("Provider=SQLOLEDB;Data Source=127.0.0.1;Initial Catalog=SemanticDB;User Id=cs267;Password=harmonic");

	if (bRtn)
	{
		ADODB::_RecordsetPtr rs = NULL;
		ExecuteSQL_IntoRecordset(rs,"SELECT * FROM URLPages");

		while(!(rs->EndOfFile))
		{
			CComVariant cv = rs->Fields->GetItem("Location")->GetValue();

			_bstr_t bs = cv.bstrVal;
			OutputDebugString(bs);

			rs->MoveNext();
		}

		string sql="exec sp_DBInsert URLPages, 'ID, URL, Location','''13'', ''URL13'', ''LOC13'' '";
		ExecuteSQL(sql);
	}

	return bRtn;
}
