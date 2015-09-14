#include "StdAfx.h"
#include "URLPage.h"


CURLPage::CURLPage(long id, const string& pFilename, const string& pURL)
{
	Clear();
	Set(id,pFilename,pURL);
}

CURLPage::CURLPage(void)
{
	Clear();
}

CURLPage::~CURLPage(void)
{
}


void CURLPage::Clear(void)
{
	mID = 0;
	mURL = mFilename = "";
	mWordCount = 0;
	mIgnore = false;
}

void CURLPage::Set(long id, const string& pFilename, const string& pURL)
{
	mID = id;
	mURL = pURL;
	mFilename = pFilename;
}

