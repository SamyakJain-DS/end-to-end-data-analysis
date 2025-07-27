# 🚀 Deployment

All major components were deployed to the cloud, making the project production-ready and accessible end-to-end:

## 🗃️ Database
- **Platform**: Aiven
- **Service**: MySQL and OLAP warehouse
- **Features**: Managed backups, high availability, and secure access

## 🌐 Flask API
- **Platform**: Render
- **Deployment**: Git-integrated CI/CD — every push triggers auto-build and deploy
- **Server**: Gunicorn (`gunicorn app:app`)
- **Security**: HTTPS support, secret environment variables for credentials

## 📊 Streamlit App
- **Platform**: Streamlit Community Cloud
- **Integration**: Linked to GitHub repo for instant redeploys
- **Public Access**: App hosted with a shareable URL

> 🔐 Coordinated multi-platform deployment — managed credentials, API communication, and environment configs to ensure seamless production rollout.

✅ **Outcome**: A fully-deployed, cloud-based data product accessible to users, demonstrating real-world engineering and deployment skills.
