import requests
import striim_tools as st

client = st.Striim()
stmt = "DESCRIBE sarah_moolai.Trial5_FileReaderSource;"

r = requests.post(f"{st.STRIIM_URL}/api/v2/tungsten",
                  headers={"Authorization": f"token {client.token}",
                           "Content-Type": "text/plain"},
                  data=stmt, timeout=30)
print(r.status_code)
print(r.text[:2000])