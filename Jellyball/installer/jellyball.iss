; Jellyball Windows installer (Inno Setup 6).
;
; Compiled by build-installer.ps1, which passes:
;   /DAppVersion=<version from version.py>
;   /DSourceDir=<absolute path to dist\Jellyball produced by jellyball.spec>
;
; Installs the PyInstaller ONEDIR payload, registers "Jellyball" as a Windows
; service running as the virtual account NT SERVICE\Jellyball, and writes a
; one-time configuration file at %ProgramData%\Jellyball\.env.
;
; Unattended installs take optional switches. /PORT, /USER and /LAN only seed a
; NEW .env (an existing one is never rewritten); /DATADIR selects the data
; folder and does not move existing data, so omit it when upgrading:
;   /PORT=8000          server port (1-65535)
;   /USER=admin         dashboard username
;   /LAN=1|0            1 = listen on the network (default), 0 = this computer only
;   /DATADIR="D:\Data"  data folder instead of %ProgramData%\Jellyball (an empty
;                       or new folder; the service gets it through its own
;                       JELLYBALL_DATA_DIR environment value)
; There is deliberately no password switch (command lines leak into logs and
; process lists). A silent install without a password lets Jellyball generate
; one on first start in <data folder>\dashboard-password.txt; the installer
; logs that path (visible with /LOG=...) and never the password itself.

#ifndef AppVersion
  #define AppVersion "0.0.0"
#endif
#ifndef SourceDir
  #define SourceDir "..\dist\Jellyball"
#endif

#define MyAppName "Jellyball"
#define MyServiceName "Jellyball"

[Setup]
AppId={{A4DA174F-565F-4137-B11D-FF47B59F9DF6}
AppName={#MyAppName}
AppVersion={#AppVersion}
AppPublisher=Jellyball
DefaultDirName={autopf}\Jellyball
DefaultGroupName=Jellyball
DisableProgramGroupPage=yes
PrivilegesRequired=admin
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
SetupIconFile=..\assets\jellyball.ico
UninstallDisplayIcon={app}\Jellyball.exe
OutputDir=Output
OutputBaseFilename=JellyballSetup-{#AppVersion}
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
CloseApplications=no
RestartApplications=no

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion

[Icons]
Name: "{group}\Uninstall Jellyball"; Filename: "{uninstallexe}"

[UninstallDelete]
Type: files; Name: "{group}\Jellyball Dashboard.url"

[UninstallRun]
Filename: "{sys}\sc.exe"; Parameters: "stop {#MyServiceName}"; Flags: runhidden waituntilterminated; RunOnceId: "JellyballStopService"
Filename: "{sys}\sc.exe"; Parameters: "delete {#MyServiceName}"; Flags: runhidden waituntilterminated; RunOnceId: "JellyballDeleteService"
Filename: "{sys}\netsh.exe"; Parameters: "advfirewall firewall delete rule name=""Jellyball"""; Flags: runhidden waituntilterminated; RunOnceId: "JellyballDeleteFirewallRule"

[Code]
var
  ConfigPage: TInputQueryWizardPage;
  NetworkPage: TInputOptionWizardPage;
  IsUpgrade: Boolean;
  UninstallDataDir: String;

const
  ServiceName = 'Jellyball';
  ServiceKey = 'SYSTEM\CurrentControlSet\Services\Jellyball';
  DataDirEnvName = 'JELLYBALL_DATA_DIR';

{ ---------------------------------------------------------------------- }
{ Helpers                                                                 }
{ ---------------------------------------------------------------------- }

function DefaultDataDir(): String;
begin
  Result := ExpandConstant('{commonappdata}\Jellyball');
end;

// The data folder recorded in the service's own Environment value (set by an
// earlier install that used /DATADIR), or '' when the default is in use.
function ReadServiceDataDir(): String;
var
  Raw, Item: String;
  P: Integer;
begin
  Result := '';
  if not RegQueryMultiStringValue(HKLM, ServiceKey, 'Environment', Raw) then
    Exit;
  while Raw <> '' do
  begin
    P := Pos(#0, Raw);
    if P = 0 then
    begin
      Item := Raw;
      Raw := '';
    end
    else
    begin
      Item := Copy(Raw, 1, P - 1);
      Raw := Copy(Raw, P + 1, MaxInt);
    end;
    if CompareText(Copy(Item, 1, Length(DataDirEnvName) + 1), DataDirEnvName + '=') = 0 then
    begin
      Result := Copy(Item, Length(DataDirEnvName) + 2, MaxInt);
      Exit;
    end;
  end;
end;

// Where Jellyball keeps .env, the database and the generated password file:
// /DATADIR if given, else the folder an earlier install recorded, else the
// default.
function GetDataDir(): String;
begin
  Result := RemoveBackslashUnlessRoot(Trim(ExpandConstant('{param:DATADIR|}')));
  if Result = '' then
    Result := ReadServiceDataDir();
  if Result = '' then
    Result := DefaultDataDir();
end;

function GetEnvFilePath(): String;
begin
  Result := AddBackslash(GetDataDir()) + '.env';
end;

// /DATADIR is the target of takeown/icacls (and, on uninstall, an optional
// recursive delete), so refuse anything that could be somebody else's folder:
// it must be absolute, not a drive root, and either new, empty, or already a
// Jellyball data folder.
function DataDirParamProblem(): String;
var
  Dir: String;
  FindRec: TFindRec;
begin
  Result := '';
  Dir := GetDataDir();
  if Dir = DefaultDataDir() then
    Exit;
  if (Length(Dir) < 4) or (Copy(Dir, 2, 2) <> ':\') then
  begin
    Result := '/DATADIR must be an absolute folder path such as D:\JellyballData (not a drive root): ' + Dir;
    Exit;
  end;
  if (not DirExists(Dir)) or FileExists(AddBackslash(Dir) + '.env') or FileExists(AddBackslash(Dir) + 'sports_proxy.db') then
    Exit;
  if FindFirst(AddBackslash(Dir) + '*', FindRec) then
  begin
    try
      repeat
        if (FindRec.Name <> '.') and (FindRec.Name <> '..') then
        begin
          Result := '/DATADIR points at a folder that is not empty and is not a Jellyball data folder: ' + Dir;
          Exit;
        end;
      until not FindNext(FindRec);
    finally
      FindClose(FindRec);
    end;
  end;
end;

function SwitchIsTrue(const Value: String): Boolean;
begin
  Result := (CompareText(Value, '1') = 0) or (CompareText(Value, 'true') = 0) or (CompareText(Value, 'yes') = 0);
end;

function SwitchIsFalse(const Value: String): Boolean;
begin
  Result := (CompareText(Value, '0') = 0) or (CompareText(Value, 'false') = 0) or (CompareText(Value, 'no') = 0);
end;

function SwitchProblem(): String;
var
  PortText, LanText, UserText: String;
  PortNum: Longint;
begin
  Result := '';
  PortText := Trim(ExpandConstant('{param:PORT|}'));
  if PortText <> '' then
  begin
    PortNum := StrToIntDef(PortText, -1);
    if (PortNum < 1) or (PortNum > 65535) then
    begin
      Result := '/PORT must be a number between 1 and 65535.';
      Exit;
    end;
  end;
  LanText := Trim(ExpandConstant('{param:LAN|}'));
  if (LanText <> '') and (not SwitchIsTrue(LanText)) and (not SwitchIsFalse(LanText)) then
  begin
    Result := '/LAN must be 1 or 0.';
    Exit;
  end;
  UserText := ExpandConstant('{param:USER|}');
  if (Pos('"', UserText) > 0) or (Pos(#13, UserText) > 0) or (Pos(#10, UserText) > 0) then
  begin
    Result := '/USER cannot contain quotes or line breaks.';
    Exit;
  end;
  Result := DataDirParamProblem();
end;

function InitializeSetup(): Boolean;
var
  Problem: String;
begin
  Result := True;
  Problem := SwitchProblem();
  if Problem <> '' then
  begin
    Log('Invalid installer switch: ' + Problem);
    SuppressibleMsgBox(Problem, mbError, MB_OK, IDOK);
    Result := False;
  end;
end;

// Quote a value for a KEY="VALUE" line the way python-dotenv expects a
// double-quoted value to be escaped (backslash, then double quote).
function EscapeEnvValue(const Value: String): String;
var
  V: String;
begin
  V := Value;
  StringChangeEx(V, '\', '\\', True);
  StringChangeEx(V, '"', '\"', True);
  Result := V;
end;

// Reverse of EscapeEnvValue, for reading a value back out of an existing
// double-quoted .env line (used on upgrade, when the wizard page is skipped).
function UnescapeEnvValue(const Value: String): String;
var
  V: String;
begin
  V := Value;
  StringChangeEx(V, '\"', '"', True);
  StringChangeEx(V, '\\', '\', True);
  Result := V;
end;

function ReadEnvValue(const FileName, Key: String): String;
var
  Lines: TArrayOfString;
  I, EqPos: Integer;
  Line, K, V: String;
begin
  Result := '';
  if not LoadStringsFromFile(FileName, Lines) then
    Exit;
  for I := 0 to GetArrayLength(Lines) - 1 do
  begin
    Line := Trim(Lines[I]);
    if (Line = '') or (Copy(Line, 1, 1) = '#') then
      Continue;
    EqPos := Pos('=', Line);
    if EqPos = 0 then
      Continue;
    K := Trim(Copy(Line, 1, EqPos - 1));
    if CompareText(K, Key) <> 0 then
      Continue;
    V := Trim(Copy(Line, EqPos + 1, MaxInt));
    if (Length(V) >= 2) and (Copy(V, 1, 1) = '"') and (Copy(V, Length(V), 1) = '"') then
      V := UnescapeEnvValue(Copy(V, 2, Length(V) - 2));
    Result := V;
    Exit;
  end;
end;

function RunHidden(const Exe, Params: String): Integer;
var
  ResultCode: Integer;
begin
  if not Exec(Exe, Params, '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    ResultCode := -1;
  Result := ResultCode;
end;

function ServiceExists(const Name: String): Boolean;
begin
  Result := RunHidden('sc.exe', 'query ' + Name) = 0;
end;

// sc query's stdout, captured via cmd redirection to a temp file, so we can
// poll for "STOPPED" while waiting for a previous instance to exit.
function GetServiceQueryOutput(const Name: String): String;
var
  TempFile: String;
  ResultCode: Integer;
  Output: AnsiString;
begin
  Result := '';
  TempFile := ExpandConstant('{tmp}\jellyball_sc_query.txt');
  Exec(ExpandConstant('{cmd}'), '/C sc.exe query ' + Name + ' > "' + TempFile + '" 2>&1',
    '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if LoadStringFromFile(TempFile, Output) then
    Result := String(Output);
end;

procedure StopServiceAndWait(const Name: String);
var
  Attempts: Integer;
begin
  if not ServiceExists(Name) then
    Exit;
  RunHidden('sc.exe', 'stop ' + Name);
  Attempts := 0;
  while Attempts < 30 do
  begin
    if Pos('STOPPED', GetServiceQueryOutput(Name)) > 0 then
      Break;
    Sleep(1000);
    Attempts := Attempts + 1;
  end;
end;

function GetFinalPort(): String;
begin
  if IsUpgrade then
  begin
    Result := ReadEnvValue(GetEnvFilePath(), 'PORT');
    if Result = '' then
      Result := '8000';
  end
  else
    Result := Trim(ConfigPage.Values[0]);
end;

function GetFinalAllowNetwork(): Boolean;
begin
  if IsUpgrade then
    Result := ReadEnvValue(GetEnvFilePath(), 'JELLYBALL_HOST') = '0.0.0.0'
  else
    Result := NetworkPage.Values[0];
end;

{ ---------------------------------------------------------------------- }
{ Wizard pages                                                            }
{ ---------------------------------------------------------------------- }

procedure InitializeWizard;
begin
  IsUpgrade := FileExists(GetEnvFilePath());

  ConfigPage := CreateInputQueryPage(wpSelectDir,
    'Jellyball Configuration',
    'Set the server port and dashboard sign-in',
    'These are written once to Jellyball''s configuration file (' + GetEnvFilePath() +
    ') and are never overwritten by a later upgrade. You can change them afterwards by editing that file.');
  ConfigPage.Add('Port:', False);
  ConfigPage.Add('Dashboard username:', False);
  ConfigPage.Add('Dashboard password:', True);
  ConfigPage.Add('Confirm password:', True);
  ConfigPage.Values[0] := '8000';
  ConfigPage.Values[1] := 'admin';
  if Trim(ExpandConstant('{param:PORT|}')) <> '' then
    ConfigPage.Values[0] := Trim(ExpandConstant('{param:PORT|}'));
  if Trim(ExpandConstant('{param:USER|}')) <> '' then
    ConfigPage.Values[1] := Trim(ExpandConstant('{param:USER|}'));

  NetworkPage := CreateInputOptionPage(ConfigPage.ID,
    'Network Access',
    'Allow other computers on your network to connect?',
    'Jellyfin and other client devices on a different machine need network access to reach Jellyball. ' +
    'If disabled, only applications on this computer can connect.',
    False, False);
  NetworkPage.Add('Allow access from other computers on my network');
  NetworkPage.Values[0] := not SwitchIsFalse(Trim(ExpandConstant('{param:LAN|}')));
end;

function ShouldSkipPage(PageID: Integer): Boolean;
begin
  Result := IsUpgrade and ((PageID = ConfigPage.ID) or (PageID = NetworkPage.ID));
end;

function NextButtonClick(CurPageID: Integer): Boolean;
var
  PortNum: Longint;
begin
  Result := True;
  if CurPageID <> ConfigPage.ID then
    Exit;

  PortNum := StrToIntDef(Trim(ConfigPage.Values[0]), -1);
  if (PortNum < 1) or (PortNum > 65535) then
  begin
    MsgBox('Please enter a valid port number between 1 and 65535.', mbError, MB_OK);
    Result := False;
    Exit;
  end;
  if Trim(ConfigPage.Values[1]) = '' then
  begin
    MsgBox('Please enter a dashboard username.', mbError, MB_OK);
    Result := False;
    Exit;
  end;
  if ConfigPage.Values[2] = '' then
  begin
    MsgBox('Please enter a dashboard password.', mbError, MB_OK);
    Result := False;
    Exit;
  end;
  if ConfigPage.Values[2] <> ConfigPage.Values[3] then
  begin
    MsgBox('The password and confirmation do not match.', mbError, MB_OK);
    Result := False;
    Exit;
  end;
  if (Pos(#13, ConfigPage.Values[2]) > 0) or (Pos(#10, ConfigPage.Values[2]) > 0) then
  begin
    MsgBox('The password cannot contain line breaks.', mbError, MB_OK);
    Result := False;
    Exit;
  end;
end;

procedure CurPageChanged(CurPageID: Integer);
var
  Port, Host, Msg: String;
begin
  if CurPageID <> wpFinished then
    Exit;
  Port := GetFinalPort();
  Host := ExpandConstant('{computername}');
  Msg := 'Jellyball is installed and running as a Windows service.' + #13#10 + #13#10 +
    'In Jellyfin, add these as your Live TV tuner and guide data provider' + #13#10 +
    '(replace the hostname if Jellyfin runs on a different computer):' + #13#10 + #13#10 +
    '  M3U:   http://' + Host + ':' + Port + '/playlist.m3u' + #13#10 +
    '  XMLTV: http://' + Host + ':' + Port + '/epg.xml';
  WizardForm.FinishedLabel.Caption := Msg;
  WizardForm.FinishedLabel.AutoSize := True;
end;

{ ---------------------------------------------------------------------- }
{ Post-install: config file, service, ACLs, firewall, shortcut            }
{ ---------------------------------------------------------------------- }

procedure WriteEnvFileIfAbsent;
var
  EnvPath, DataDir, Host, Contents, PasswordLine: String;
begin
  DataDir := GetDataDir();
  ForceDirectories(DataDir);
  EnvPath := GetEnvFilePath();
  if FileExists(EnvPath) then
    Exit;

  if NetworkPage.Values[0] then
    Host := '0.0.0.0'
  else
    Host := '127.0.0.1';

  { An unattended install has no password page. Leave DASHBOARD_PASSWORD out
    rather than writing an empty value: on a network bind Jellyball then
    generates a random password on first start and saves it to
    dashboard-password.txt in the data folder (on loopback there is none, and
    the dashboard is only reachable from this computer). Never print the
    password; only say where the file will be. }
  if ConfigPage.Values[2] <> '' then
    PasswordLine := 'DASHBOARD_PASSWORD="' + EscapeEnvValue(ConfigPage.Values[2]) + '"' + #13#10
  else
  begin
    PasswordLine := '# DASHBOARD_PASSWORD is not set: Jellyball generates one on first start.' + #13#10;
    if NetworkPage.Values[0] then
      Log('No dashboard password was supplied; Jellyball will generate one on first start in ' +
        AddBackslash(DataDir) + 'dashboard-password.txt (user ' + Trim(ConfigPage.Values[1]) + ').')
    else
      Log('No dashboard password was supplied; the dashboard is reachable only from this computer.');
  end;

  Contents :=
    '# Jellyball configuration - written once by the installer.' + #13#10 +
    '# This file is never overwritten by an upgrade; edit it directly and' + #13#10 +
    '# restart the "Jellyball" service to apply changes. See README.md for' + #13#10 +
    '# the full list of supported settings.' + #13#10 +
    'PORT="' + EscapeEnvValue(Trim(ConfigPage.Values[0])) + '"' + #13#10 +
    'JELLYBALL_HOST="' + EscapeEnvValue(Host) + '"' + #13#10 +
    'DASHBOARD_USERNAME="' + EscapeEnvValue(Trim(ConfigPage.Values[1])) + '"' + #13#10 +
    PasswordLine +
    'MULTIVIEW_HWACCEL=nvenc' + #13#10 + #13#10 +
    '# Optional settings (uncomment and edit as needed):' + #13#10 +
    '#JELLYFIN_URL=http://127.0.0.1:8096' + #13#10 +
    '#JELLYFIN_API_KEY=' + #13#10 +
    '#DISCORD_WEBHOOK_URL=' + #13#10 +
    '#STREAM_MAX_BANDWIDTH=' + #13#10;

  SaveStringToFile(EnvPath, Contents, False);
end;

procedure ConfigureDataDirAcls;
var
  DataDir, EnvPath: String;
begin
  { Must run AFTER the service exists: the virtual account NT SERVICE\Jellyball
    is only resolvable once the service is created, and icacls rejects the
    whole command otherwise (which previously left the .env - with the
    dashboard password - readable by every local user). The data folder
    (settings, database, logs) is limited to Administrators, SYSTEM and the
    service; the .env is read-only for the service. }
  DataDir := GetDataDir();
  ForceDirectories(DataDir);
  { Protect the folder itself with inheritable grants, then reset everything
    inside it to inherit from the folder. (Applying /inheritance:r with the
    folder-style grants recursively via /T stripped existing files' inherited
    ACEs without granting anything, locking the service out of its own
    database and log.) }
  RunHidden('icacls.exe', '"' + DataDir + '" /inheritance:r /grant:r "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "NT SERVICE\Jellyball:(OI)(CI)M" /C /Q');
  { Take ownership first (takeown enables the take-ownership privilege; icacls
    /setowner does not): files the
    service created are owned by it, and a broken earlier install could leave
    them with no ACE an administrator can use to reset them. }
  RunHidden('takeown.exe', '/F "' + DataDir + '" /A /R /D Y');
  RunHidden('icacls.exe', '"' + DataDir + '\*" /reset /T /C /Q');

  EnvPath := GetEnvFilePath();
  if FileExists(EnvPath) then
    RunHidden('icacls.exe', '"' + EnvPath + '" /inheritance:r /grant:r "NT SERVICE\Jellyball:R" "*S-1-5-32-544:F" "*S-1-5-18:F" /C /Q');
end;

// Record /DATADIR in the service's own Environment value so the service (and
// every later upgrade) uses that folder. Other entries are preserved.
procedure RegisterServiceDataDir;
var
  Raw, Item, NewRaw: String;
  P: Integer;
begin
  if Trim(ExpandConstant('{param:DATADIR|}')) = '' then
    Exit;
  Raw := '';
  RegQueryMultiStringValue(HKLM, ServiceKey, 'Environment', Raw);
  NewRaw := '';
  while Raw <> '' do
  begin
    P := Pos(#0, Raw);
    if P = 0 then
    begin
      Item := Raw;
      Raw := '';
    end
    else
    begin
      Item := Copy(Raw, 1, P - 1);
      Raw := Copy(Raw, P + 1, MaxInt);
    end;
    if (Item <> '') and (CompareText(Copy(Item, 1, Length(DataDirEnvName) + 1), DataDirEnvName + '=') <> 0) then
      NewRaw := NewRaw + Item + #0;
  end;
  NewRaw := NewRaw + DataDirEnvName + '=' + GetDataDir();
  if not RegWriteMultiStringValue(HKLM, ServiceKey, 'Environment', NewRaw) then
    Log('Could not record the data folder in the service environment.');
end;

procedure RegisterService;
var
  BinPath, Params: String;
begin
  BinPath := '\"' + ExpandConstant('{app}\Jellyball.exe') + '\" --service';
  if ServiceExists(ServiceName) then
    Params := 'config ' + ServiceName + ' binPath= "' + BinPath + '" start= delayed-auto obj= "NT SERVICE\Jellyball" DisplayName= "Jellyball Sports Proxy"'
  else
    Params := 'create ' + ServiceName + ' binPath= "' + BinPath + '" start= delayed-auto obj= "NT SERVICE\Jellyball" DisplayName= "Jellyball Sports Proxy"';
  RunHidden('sc.exe', Params);
  RunHidden('sc.exe', 'description ' + ServiceName + ' "Jellyfin Live TV sports proxy: M3U/XMLTV tuner, stream failover and Multi-View."');
  RunHidden('sc.exe', 'failure ' + ServiceName + ' reset= 86400 actions= restart/10000/restart/30000/restart/60000');
  RunHidden('sc.exe', 'failureflag ' + ServiceName + ' 1');
end;

procedure ConfigureFirewall;
begin
  RunHidden('netsh.exe', 'advfirewall firewall delete rule name="Jellyball"');
  if GetFinalAllowNetwork() then
    RunHidden('netsh.exe', 'advfirewall firewall add rule name="Jellyball" dir=in action=allow protocol=TCP localport=' +
      GetFinalPort() + ' profile=private,domain remoteip=localsubnet');
end;

procedure CreateDashboardShortcutFile;
var
  GroupDir, UrlPath, Contents: String;
begin
  GroupDir := ExpandConstant('{group}');
  ForceDirectories(GroupDir);
  UrlPath := GroupDir + '\Jellyball Dashboard.url';
  Contents := '[InternetShortcut]' + #13#10 + 'URL=http://localhost:' + GetFinalPort() + '/' + #13#10;
  SaveStringToFile(UrlPath, Contents, False);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssInstall then
    StopServiceAndWait(ServiceName);

  if CurStep = ssPostInstall then
  begin
    WriteEnvFileIfAbsent;
    RegisterService;
    RegisterServiceDataDir;
    ConfigureDataDirAcls;
    ConfigureFirewall;
    RunHidden('sc.exe', 'start ' + ServiceName);
    CreateDashboardShortcutFile;
  end;
end;

{ ---------------------------------------------------------------------- }
{ Uninstall: optionally remove settings/database                         }
{ ---------------------------------------------------------------------- }

function InitializeUninstall(): Boolean;
begin
  { Read before the service (and its Environment value) is deleted. }
  UninstallDataDir := ReadServiceDataDir();
  if UninstallDataDir = '' then
    UninstallDataDir := DefaultDataDir();
  Result := True;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  DataDir: String;
begin
  if CurUninstallStep <> usPostUninstall then
    Exit;
  DataDir := UninstallDataDir;
  if not DirExists(DataDir) then
    Exit;
  if MsgBox('Also delete Jellyball''s settings and database in ' + DataDir + '?' + #13#10 +
    'This cannot be undone.', mbConfirmation, MB_YESNO or MB_DEFBUTTON2) = IDYES then
    DelTree(DataDir, True, True, True);
end;
