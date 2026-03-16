export default async (request, context) => {
    const corsHeaders = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    };

    // Handle CORS preflight requests
    if (request.method === "OPTIONS") {
        return new Response("ok", { headers: corsHeaders });
    }

    // Load sensitive keys from Netlify Environment Variables
    const LARK_APP_ID = Netlify.env.get("LARK_APP_ID");
    const LARK_APP_SECRET = Netlify.env.get("LARK_APP_SECRET");
    const LARK_APP_TOKEN = Netlify.env.get("LARK_APP_TOKEN");
    const LARK_TABLE_ID = Netlify.env.get("LARK_LICENSE_TABLE_ID");

    if (!LARK_APP_SECRET || !LARK_TABLE_ID) {
        return new Response(JSON.stringify({ success: false, msg: "Server missing Env Vars" }), { status: 500, headers: corsHeaders });
    }

    try {
        // 1. Fetch Authorization Token from Lark
        const tokenRes = await fetch("https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ app_id: LARK_APP_ID, app_secret: LARK_APP_SECRET })
        });
        const tokenData = await tokenRes.json();
        const token = tokenData.tenant_access_token;

        const contentType = request.headers.get("content-type") || "";

        // --- ACTION 1: VERIFY LICENSE (JSON Request) ---
        if (contentType.includes("application/json")) {
            const body = await request.json();
            
            if (body.action === "verify") {
                const searchRes = await fetch(`https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_TABLE_ID}/records/search`, {
                    method: "POST",
                    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
                    body: JSON.stringify({
                        filter: { conjunction: "and", conditions: [{ field_name: "Device ID", operator: "is", value: [body.deviceId] }] }
                    })
                });
                const searchData = await searchRes.json();
                
                // Return just the data part to the frontend
                return new Response(JSON.stringify({ success: true, data: searchData.data }), { headers: corsHeaders });
            }
        } 
        
        // --- ACTION 2: SUBMIT LICENSE REQUEST (FormData / File Upload Request) ---
        else if (contentType.includes("multipart/form-data")) {
            const formData = await request.formData();
            
            if (formData.get("action") === "submit") {
                const deviceId = formData.get("deviceId");
                const contact = formData.get("contact");
                const file = formData.get("file"); // This is the image file

                // 2a. Upload image to Lark Drive
                const larkFormData = new FormData();
                larkFormData.append("file_name", file.name);
                larkFormData.append("parent_type", "bitable_image");
                larkFormData.append("parent_node", LARK_APP_TOKEN); 
                larkFormData.append("size", file.size);
                larkFormData.append("file", file);

                const uploadRes = await fetch("https://open.larksuite.com/open-apis/drive/v1/medias/upload_all", {
                    method: "POST",
                    headers: { "Authorization": `Bearer ${token}` }, // Deno handles the multipart boundary automatically
                    body: larkFormData
                });
                
                const uploadData = await uploadRes.json();
                if (uploadData.code !== 0) throw new Error("File Upload Failed: " + JSON.stringify(uploadData));
                const fileToken = uploadData.data.file_token;

                // 2b. Create Record in Bitable
                const createRes = await fetch(`https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_TABLE_ID}/records`, {
                    method: "POST",
                    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
                    body: JSON.stringify({
                        fields: {
                            "Device ID": deviceId,
                            "Name/WechatID/Phone": contact,
                            "Status": "Pending",
                            "Attachment": [{ "file_token": fileToken }]
                        }
                    })
                });

                const createData = await createRes.json();
                if (createData.code !== 0) throw new Error("Record Creation Failed");

                return new Response(JSON.stringify({ success: true }), { headers: corsHeaders });
            }
        }

        return new Response(JSON.stringify({ success: false, msg: "Invalid request type" }), { status: 400, headers: corsHeaders });

    } catch (err) {
        console.error("Edge Function Error:", err);
        return new Response(JSON.stringify({ success: false, error: err.message }), { status: 500, headers: corsHeaders });
    }
};