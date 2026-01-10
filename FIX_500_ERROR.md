# 🔧 Fix for 500 Error on Vercel

## 🎯 Root Cause

Your application is crashing on Vercel because **required environment variables are not set**. The app needs `MONGO_URI` and `MONGO_DB` to connect to your MongoDB database.

## ✅ Solution (Follow These Steps)

### Step 1: Set Environment Variables in Vercel

1. **Go to Vercel Dashboard**
   - Visit: https://vercel.com/dashboard
   - Select your project: **dashboard-filtter**

2. **Navigate to Settings**
   - Click on **Settings** tab
   - Click on **Environment Variables** in the left sidebar

3. **Add Required Variables**

   Click "Add New" and add each of these:

   **MONGO_URI**
   ```
   mongodb+srv://your_username:your_password@your_cluster.mongodb.net/?retryWrites=true&w=majority
   ```
   - Select: Production, Preview, Development

   **MONGO_DB**
   ```
   your_database_name
   ```
   - Select: Production, Preview, Development

4. **Optional: Add Teams Database Variables** (if you have a separate teams database)

   **TEAMS_MONGO_URI**
   ```
   mongodb+srv://...
   ```

   **TEAMS_MONGO_DB**
   ```
   teams_database_name
   ```

### Step 2: Redeploy

After adding environment variables:

**Option A: Automatic Redeploy**
```bash
git commit --allow-empty -m "Trigger redeploy"
git push
```

**Option B: Manual Redeploy**
1. Go to **Deployments** tab in Vercel
2. Click the three dots (...) on the latest deployment
3. Click **Redeploy**

### Step 3: Verify It's Working

1. **Check Health Endpoint**
   - Visit: https://dashboard-filtter.vercel.app/health
   - Should return: `{"status": "healthy", "database": "connected"}`

2. **Check Main Dashboard**
   - Visit: https://dashboard-filtter.vercel.app/
   - Should load without errors

## 🔍 Still Not Working?

### Check Vercel Function Logs

1. Go to your deployment in Vercel
2. Click on the **Functions** tab
3. Look for error messages

### Common Issues & Solutions

#### Issue 1: "MongoServerError: bad auth"
**Problem**: Wrong username/password
**Solution**: Double-check your MongoDB credentials

#### Issue 2: "IP not whitelisted"
**Problem**: Vercel's IPs are not allowed in MongoDB
**Solution**:
1. Go to MongoDB Atlas
2. Navigate to **Network Access**
3. Click **Add IP Address**
4. Select **Allow Access from Anywhere** (0.0.0.0/0)
5. Click **Confirm**

#### Issue 3: "Database timeout"
**Problem**: Can't reach MongoDB
**Solution**:
- Verify your connection string format
- Check MongoDB cluster is running
- Ensure `retryWrites=true&w=majority` is in your connection string

#### Issue 4: Special characters in password
**Problem**: Password contains special characters
**Solution**: URL-encode special characters in your password
- Example: `p@ssw0rd!` becomes `p%40ssw0rd%21`
- Use online URL encoder or:
  ```python
  from urllib.parse import quote_plus
  password = quote_plus("p@ssw0rd!")
  ```

## 🧪 Test Locally First

Before redeploying to Vercel, test locally:

1. **Create `.env` file**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your values**
   ```env
   MONGO_URI=mongodb+srv://...
   MONGO_DB=your_database_name
   ```

3. **Run verification script**
   ```bash
   python verify_setup.py
   ```

4. **Start the app**
   ```bash
   python app.py
   ```

5. **Test locally**
   - Visit: http://localhost:5000
   - Should work without errors

## 📋 Checklist

- [ ] Environment variables set in Vercel
- [ ] MongoDB IP whitelist configured
- [ ] Redeployed the application
- [ ] `/health` endpoint returns healthy status
- [ ] Main dashboard loads without errors

## 💡 What Was Fixed

The following improvements were made to your codebase:

1. **Better Error Handling** (`app.py`)
   - Added health check endpoint: `/health`
   - Added error handlers for 500 and 404 errors
   - Better error messages

2. **Improved Database Error Messages** (`db.py`)
   - Clearer error messages with step-by-step instructions
   - Helpful emojis and formatting

3. **Error Template** (`templates/error.html`)
   - User-friendly error page
   - Helpful troubleshooting tips

4. **Documentation**
   - `README.md`: Comprehensive project documentation
   - `DEPLOYMENT.md`: Detailed deployment guide
   - `FIX_500_ERROR.md`: This quick fix guide
   - `verify_setup.py`: Setup verification script

## 🆘 Need More Help?

1. Run the verification script locally:
   ```bash
   python verify_setup.py
   ```

2. Check Vercel logs for specific error messages

3. Refer to `DEPLOYMENT.md` for detailed troubleshooting

4. Test the `/health` endpoint to verify database connectivity

---

**After following these steps, your 500 error should be resolved! 🎉**
