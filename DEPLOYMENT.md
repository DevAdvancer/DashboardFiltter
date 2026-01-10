# Deployment Guide for Dashboard Filter

## 🚀 Vercel Deployment Steps

### Step 1: Set Environment Variables

Your 500 error is caused by missing environment variables. Follow these steps:

1. Go to [Vercel Dashboard](https://vercel.com/dashboard)
2. Select your project: **dashboard-filtter**
3. Click on **Settings** tab
4. Click on **Environment Variables** in the left sidebar
5. Add the following variables:

#### Required Variables:
```
MONGO_URI=mongodb+srv://your_username:your_password@your_cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=your_database_name
```

#### Optional Variables (if using separate teams database):
```
TEAMS_MONGO_URI=mongodb+srv://...
TEAMS_MONGO_DB=teams_database_name
```

#### Additional Configuration (optional):
```
FLASK_DEBUG=False
FLASK_PORT=5000
```

### Step 2: Redeploy

After setting the environment variables:

1. Go to the **Deployments** tab
2. Click on the three dots (...) on the latest deployment
3. Click **Redeploy**
4. OR simply push a new commit to trigger a deployment

### Step 3: Verify Deployment

Once deployed, check:

1. Visit: `https://dashboard-filtter.vercel.app/health`
   - Should return: `{"status": "healthy", "database": "connected"}`

2. Visit: `https://dashboard-filtter.vercel.app/`
   - Should load the dashboard without errors

## 🔍 Troubleshooting

### Still Getting 500 Error?

1. **Check Vercel Logs**:
   - Go to your deployment
   - Click on the **Functions** tab
   - Look for error messages in the logs

2. **Verify MongoDB Connection String**:
   - Make sure your IP is whitelisted in MongoDB Atlas
   - MongoDB Atlas → Network Access → Add IP Address → Allow Access from Anywhere (0.0.0.0/0)

3. **Check MongoDB Credentials**:
   - Ensure username and password are correct
   - Special characters in password should be URL-encoded

4. **Database Name**:
   - Verify the database name exists in your MongoDB cluster

### Common Issues:

#### Issue: "MongoServerError: bad auth"
**Solution**: Check your MongoDB username and password

#### Issue: "MongoServerError: IP not whitelisted"
**Solution**: Add Vercel's IP ranges to MongoDB Atlas network access

#### Issue: "Database connection timeout"
**Solution**:
- Check MongoDB cluster is running
- Verify connection string format
- Ensure retryWrites parameter is included

## 📋 Local Development

To run locally:

1. Create a `.env` file in the project root:
```bash
MONGO_URI=mongodb+srv://...
MONGO_DB=your_database_name
FLASK_DEBUG=True
FLASK_PORT=5000
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python app.py
```

4. Visit: `http://localhost:5000`

## 🔐 Security Best Practices

1. **Never commit `.env` file** (already in .gitignore)
2. **Use strong passwords** for MongoDB
3. **Restrict IP access** in MongoDB Atlas (after testing)
4. **Use environment-specific credentials**
5. **Enable MongoDB authentication**

## 📊 Monitoring

### Health Check Endpoint
```
GET /health
```
Returns the status of the application and database connection.

### Expected Response:
```json
{
  "status": "healthy",
  "database": "connected"
}
```

## 🆘 Need Help?

If you're still experiencing issues:

1. Check the Vercel function logs
2. Test the `/health` endpoint
3. Verify all environment variables are set correctly
4. Ensure MongoDB cluster is accessible

## 📝 Environment Variable Template

Copy this template to your Vercel Environment Variables:

```
# Required
MONGO_URI=
MONGO_DB=

# Optional (for separate teams database)
TEAMS_MONGO_URI=
TEAMS_MONGO_DB=

# Optional (Flask configuration)
FLASK_DEBUG=False
```
