export default async (request, context) => {
    // 1. Setup CORS & Prevent Caching (Crucial for real-time sync across devices)
    const corsHeaders = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    };

    if (request.method === "OPTIONS") {
        return new Response("ok", { status: 200, headers: corsHeaders });
    }

    const url = new URL(request.url);
    const deviceId = url.searchParams.get("deviceId");
    
    // Kept exactly as your working code
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

        // HELPER: Safely extract text from Lark's complex field formats (fixes the alias extraction bug)
        const extractLarkText = (fieldData) => {
            if (!fieldData) return "";
            if (Array.isArray(fieldData)) return fieldData.map(item => item.text || "").join("").trim();
            if (typeof fieldData === "object" && fieldData !== null) return (fieldData.text || "").trim();
            return String(fieldData).trim();
        };

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
        let effectiveDeviceId = deviceId; // Defaults to the user's own ID

        if (licenseRecord) {
            const statusStr = extractLarkText(licenseRecord.fields["Status"]);
            if (statusStr.toLowerCase().includes("active")) {
                isLicensed = true;

                // --- MASTER ALIAS LOGIC ---
                // If you set a "Master Device ID" in the License table, it safely extracts it and swaps IDs.
                const masterStr = extractLarkText(licenseRecord.fields["Master Device ID"]);
                if (masterStr !== "" && masterStr.toLowerCase() !== "undefined") {
                    effectiveDeviceId = masterStr;
                }
            }
        }

        if (!isLicensed) {
            return new Response(JSON.stringify({success: false, msg: "Unauthorized"}), { status: 403, headers: corsHeaders });
        }

        // 5. Search for existing backup (using the Effective ID)
        // BUG FIX: Removed 'App Name' from Lark's strict filter. We fetch all records for the device and filter in JS to bypass Case-Sensitivity issues.
        const searchRes = await fetch(`https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_SYNC_TABLE_ID}/records/search`, {
            method: "POST",
            headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
            body: JSON.stringify({ 
                filter: { 
                    conjunction: "and", 
                    conditions: [
                        { field_name: "Device ID", operator: "is", value: [effectiveDeviceId] }
                    ] 
                } 
            })
        });
        const searchData = await searchRes.json();
        const allRecords = searchData.data?.items || [];

        // Case-insensitive match for the app name ("Hub" === "hub")
        const existingRecord = allRecords.find(r => {
            const rAppName = extractLarkText(r.fields["App Name"]);
            return rAppName.toLowerCase() === appName.toLowerCase();
        });

        // --- HANDLE RESTORE (GET) ---
        if (request.method === "GET") {
            const defaultEmpty = appName === "strategy" ? "{}" : "[]";
            if (!existingRecord) {
                return new Response(JSON.stringify({ success: true, data: defaultEmpty }), { headers: corsHeaders });
            }
            
            let profilesField = existingRecord.fields["Profiles Data"];
            let fullText = defaultEmpty;
            
            // Look for the file attachment token in the field
            if (profilesField && Array.isArray(profilesField) && profilesField[0]?.file_token) {
                try {
                    const fileToken = profilesField[0].file_token;
                    // Download the JSON file content from Lark Drive
                    const dlRes = await fetch(`https://open.larksuite.com/open-apis/drive/v1/medias/${fileToken}/download`, {
                        headers: { "Authorization": `Bearer ${token}` }
                    });
                    if (dlRes.ok) {
                        fullText = await dlRes.text();
                    } else {
                        console.error("Failed to download attachment from Lark Drive");
                    }
                } catch(e) {
                    console.error("Failed to parse profile attachment", e);
                }
            } else {
                // Fallback for old text records if any still exist
                if (Array.isArray(profilesField)) {
                    fullText = profilesField.map(segment => segment.text || "").join("");
                } else if (typeof profilesField === 'object' && profilesField !== null) {
                    fullText = profilesField.text || "";
                } else {
                    fullText = String(profilesField || defaultEmpty);
                }
            }
            
            return new Response(JSON.stringify({ success: true, data: fullText }), { headers: corsHeaders });
        }

        // --- HANDLE BACKUP (POST) ---
        if (request.method === "POST") {
            const body = await request.json();
            const profilesString = JSON.stringify(body.profiles);

            // 1. Upload the File to Lark Drive safely
            const textBytes = new TextEncoder().encode(profilesString);
            const blob = new Blob([textBytes], { type: 'application/json' });

            const larkFormData = new FormData();
            larkFormData.append("file_name", `profiles_${appName}.json`);
            larkFormData.append("parent_type", "bitable_file"); 
            larkFormData.append("parent_node", LARK_APP_TOKEN);
            larkFormData.append("size", blob.size.toString());
            larkFormData.append("file", blob, `profiles_${appName}.json`); // Safe filename formatting

            const uploadRes = await fetch("https://open.larksuite.com/open-apis/drive/v1/medias/upload_all", {
                method: "POST",
                headers: { "Authorization": `Bearer ${token}` },
                body: larkFormData
            });

            const uploadData = await uploadRes.json();
            if (uploadData.code !== 0) throw new Error("File Upload Failed: " + JSON.stringify(uploadData));
            const fileToken = uploadData.data.file_token;

            // 2. Prepare Bitable Payload
            const readableDate = new Intl.DateTimeFormat('en-GB', {
                timeZone: 'Asia/Kuala_Lumpur',
                day: '2-digit', month: '2-digit', year: 'numeric',
                hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
            }).format(new Date());

            let payload = {
                fields: {
                    "Device ID": effectiveDeviceId, // Writes explicitly to effective ID
                    "App Name": existingRecord ? extractLarkText(existingRecord.fields["App Name"]) : appName, // Keep original case if it exists
                    "Profiles Data": [{ "file_token": fileToken }],
                    "Last Synced": readableDate
                }
            };

            const saveRecord = async (dataPayload) => {
                const endpoint = existingRecord 
                    ? `https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_SYNC_TABLE_ID}/records/${existingRecord.record_id}`
                    : `https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_SYNC_TABLE_ID}/records`;
                const method = existingRecord ? "PUT" : "POST";
                
                const res = await fetch(endpoint, {
                    method: method,
                    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
                    body: JSON.stringify(dataPayload)
                });
                return await res.json();
            };

            let larkData = await saveRecord(payload);

            // ==========================================
            // AUTO-HEALING MECHANISMS
            // ==========================================
            
            // Heal 1: If user forgot to create the "Last Synced" column, remove it and retry
            if (larkData.code !== 0 && larkData.msg && larkData.msg.includes("Invalid field")) {
                console.warn("Auto-healing: Missing 'Last Synced' field. Retrying without it...");
                delete payload.fields["Last Synced"];
                larkData = await saveRecord(payload);
            }

            // Heal 2: If the "Profiles Data" is still a Text column (not Attachment), fallback to raw text
            if (larkData.code !== 0 && larkData.msg && (larkData.msg.includes("type") || larkData.msg.includes("attachment"))) {
                console.warn("Auto-healing: Column is not Attachment type. Falling back to text saving...");
                payload.fields["Profiles Data"] = profilesString.substring(0, 90000); 
                larkData = await saveRecord(payload);
            }

            // Final Error Check
            if (larkData.code !== 0) {
                throw new Error("Lark Rejected Save: " + larkData.msg);
            }

            return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });
        }

    } catch (err) {
        console.error("Sync Crash:", err.message);
        return new Response(JSON.stringify({ success: false, error: err.message }), { status: 500, headers: corsHeaders });
    }
};