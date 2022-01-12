API_URLS = dict(
        mcm_public_url = "https://cms-pdmv.cern.ch/mcm/public/restapi",
        mcm_private_url = "https://cms-pdmv.cern.ch/mcm/restapi",
        dbs_global_url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader",
        dbs_user_url = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader",
        phedex_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod",
        pmp_url = "https://cms-pdmv.cern.ch/pmp/api",
        xsdb_url = "https://cms-gen-dev.cern.ch/xsdb/api",
        )

import socket
if "uafino" in socket.gethostname():
    AUTH_PATHS = dict(
            capath = "/etc/grid-security/certificates/",
            cacert = "/tmp/x509up_u31693",
            usercert = "/homes/hmei/.globus/usercert.pem",
            userkey_passwordless = "/homes/hmei/.globus/userkey_nopass.pem", # convert password key to passwordless with `openssl rsa -in ~/.globus/userkey.pem  -out ~/.globus/userkey_nopass.pem`
            cookie_file = "/homes/hmei/private/ssocookie.txt", # `mkdir -p private/` ; the cookie gest created automatically
            extra_args_sso = "--cacert /etc/pki/tls/certs/CERN-bundle.pem", # inject hacky cli stuff to cern-get-sso-cookie for uafino
            )
else:
    AUTH_PATHS = dict(
            capath = "/etc/grid-security/certificates/",
            cacert = "/tmp/x509up_u31693",
            usercert = "/home/users/hmei/.globus/usercert.pem",
            userkey_passwordless = "/home/users/hmei/.globus/userkey_nopass.pem", # convert password key to passwordless with `openssl rsa -in ~/.globus/userkey.pem  -out ~/.globus/userkey_nopass.pem`
            cookie_file = "/home/users/hmei/private/ssocookie.txt", # `mkdir -p private/` ; the cookie gest created automatically
            extra_args_sso = "", # inject hacky cli stuff to cern-get-sso-cookie for uafino
            )
    #AUTH_PATHS = dict(
    #        capath = "/etc/grid-security/certificates/",
    #        cacert = "/tmp/x509up_u31567",
    #        usercert = "/home/users/namin/.globus/usercert.pem",
    #        userkey_passwordless = "/home/users/namin/.globus/userkey_nopass.pem", # convert password key to passwordless with `openssl rsa -in ~/.globus/userkey.pem  -out ~/.globus/userkey_nopass.pem`
    #        cookie_file = "/home/users/namin/private/ssocookie.txt", # `mkdir -p private/` ; the cookie gest created automatically
    #        extra_args_sso = "",
    #        )
