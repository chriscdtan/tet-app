export default async (request, context) => {
    // 1. Setup CORS
    const corsHeaders = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
        return new Response("ok", { status: 200, headers: corsHeaders });
    }

    const url = new URL(request.url);
    const deviceId = url.searchParams.get("deviceId");
    const appName = url.searchParams.get("app") || "yolo";

    if (!deviceId) {
        return new Response(JSON.stringify({success: false, msg: "Missing deviceId"}), { status: 400, headers: corsHeaders });
    }

    // 2. Load keys
    const LARK_APP_ID = Netlify.env.get("LARK_APP_ID");
    const LARK_APP_SECRET = Netlify.env.get("LARK_APP_SECRET");
    const LARK_APP_TOKEN = Netlify.env.get("LARK_APP_TOKEN");
    const LARK_SYNC_TABLE_ID = Netlify.env.get("LARK_SYNC_TABLE_ID"); 
    const LARK_LICENSE_TABLE_ID = Netlify.env.get("LARK_LICENSE_TABLE_ID"); 

    if (!LARK_APP_SECRET || !LARK_LICENSE_TABLE_ID || !LARK_SYNC_TABLE_ID) {
        return new Response(JSON.stringify({success: false, msg: "Server missing Env Vars"}), {status: 500, headers: corsHeaders});
    }

    try {
        // 3. Fetch Token
        const tokenRes = await fetch("https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ app_id: LARK_APP_ID, app_secret: LARK_APP_SECRET })
        });
        const tokenData = await tokenRes.json();
        const token = tokenData.tenant_access_token;

        // 4. SECURITY CHECK: Verify License
        const licenseRes = await fetch(`https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_LICENSE_TABLE_ID}/records/search`, {
            method: "POST",
            headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
            body: JSON.stringify({
                filter: { conjunction: "and", conditions: [{ field_name: "Device ID", operator: "is", value: [deviceId] }] }
            })
        });
        const licenseData = await licenseRes.json();
        const licenseRecord = licenseData.data?.items?.[0];

        let isLicensed = false;
        if (licenseRecord) {
            const statusRaw = licenseRecord.fields["Status"];
            let statusStr = typeof statusRaw === 'object' && statusRaw !== null ? (statusRaw.text || JSON.stringify(statusRaw)) : String(statusRaw || "");
            if (statusStr.toLowerCase().includes("active")) isLicensed = true;
        }

        if (!isLicensed) {
            return new Response(JSON.stringify({success: false, msg: "Unauthorized"}), { status: 403, headers: corsHeaders });
        }

        // 5. Search for existing backup
        const searchRes = await fetch(`https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_SYNC_TABLE_ID}/records/search`, {
            method: "POST",
            headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
            body: JSON.stringify({ 
                filter: { 
                    conjunction: "and", 
                    conditions: [
                        { field_name: "Device ID", operator: "is", value: [deviceId] },
                        { field_name: "App Name", operator: "is", value: [appName] }
                    ] 
                } 
            })
        });
        const searchData = await searchRes.json();
        const existingRecord = searchData.data?.items?.[0];

        // --- HANDLE RESTORE (GET) ---
        if (request.method === "GET") {
            if (!existingRecord) {
                return new Response(JSON.stringify({ success: true, data: "[]" }), { headers: corsHeaders });
            }
            
            let profilesJson = existingRecord.fields["Profiles Data"];
            let fullText = "";
            
            // Re-stitch the chunked text segments back together for the frontend
            if (Array.isArray(profilesJson)) {
                fullText = profilesJson.map(segment => segment.text || "").join("");
            } else if (typeof profilesJson === 'object' && profilesJson !== null) {
                fullText = profilesJson.text || "";
            } else {
                fullText = String(profilesJson || "[]");
            }
            
            return new Response(JSON.stringify({ success: true, data: fullText }), { headers: corsHeaders });
        }

        // --- HANDLE BACKUP (POST) ---
        if (request.method === "POST") {
            const body = await request.json();
            const profilesString = JSON.stringify(body.profiles);

            // 🛠️ CRITICAL FIX: Lark rejects simple strings > 1000 chars.
            // We must chunk the long JSON string into an array of Segment Objects
            const MAX_SEGMENT_LENGTH = 900;
            const profilesSegments = [];
            
            for (let i = 0; i < profilesString.length; i += MAX_SEGMENT_LENGTH) {
                profilesSegments.push({
                    type: "text",
                    text: profilesString.substring(i, i + MAX_SEGMENT_LENGTH)
                });
            }

            const payload = {
                fields: {
                    "Device ID": deviceId,
                    "App Name": appName,
                    "Profiles Data": profilesSegments,
                    "Last Synced": Date.now()
                }
            };

            let larkRes;
            if (existingRecord) {
                larkRes = await fetch(`https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_SYNC_TABLE_ID}/records/${existingRecord.record_id}`, {
                    method: "PUT",
                    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
                    body: JSON.stringify(payload)
                });
            } else {
                larkRes = await fetch(`https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_SYNC_TABLE_ID}/records`, {
                    method: "POST",
                    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
                    body: JSON.stringify(payload)
                });
            }
            
            const larkData = await larkRes.json();
            if (larkData.code !== 0) {
                console.error("Lark API Rejected the Save:", larkData);
                throw new Error("Lark Rejected Save: " + larkData.msg);
            }

            return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });
        }

    } catch (err) {
        console.error("Sync Crash:", err.message);
        return new Response(JSON.stringify({ success: false, error: err.message }), { status: 500, headers: corsHeaders });
    }
};