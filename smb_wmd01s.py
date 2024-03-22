from smb.SMBConnection import SMBConnection

server_name = "mushqmdm1pr.sw.sherwin.com"
share_name = 'e$'
username = 'wmd01s'
password = ''
domain_name = 'sw'

conn = SMBConnection(username, password, 'my_python_client', server_name, domain=domain_name, use_ntlm_v2=True,
                     sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
                     is_direct_tcp=True)
conn.connect(server_name, 445)

if conn:
    file_list = conn.listPath(share_name, '/Program Files/MDM/MDM Global System/LACG/Audits/Reports')

    for item in file_list:
        print(item.filename)
    conn.close()
else:
    print("Connection Failed.")
