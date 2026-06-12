import http from 'k6/http';
import { check, sleep } from 'k6';
//import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    let r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

export const options = {
    setupTimeout: '120s',
    scenarios: {
        load_test: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 100 },
                { duration: '1m', target: 100 },
                { duration: '30s', target: 0 },
            ],
            gracefulStop: '30s',
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<500'],
    },
};

const BASE_URL = 'http://172.16.245.5:65001/api/v1';

// Helper function to generate random strings for setup phase
function generateRandomString(length) {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) result += chars.charAt(Math.floor(Math.random() * chars.length));
    return result;
}

// Helper function to randomly shuffle an array
function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

// 1. Setup Phase: Initialize test environment by creating 10 providers concurrently
export function setup() {
    console.log("Starting Setup: Creating 10 Providers in parallel...");
    
    const providerRequests = [];
    for (let i = 0; i < 10; i++) {
        const payload = JSON.stringify({
            is_core: i === 0, // Core provider might fail with 500 if already exists, which is handled
            legal_name: `Legal Name ${generateRandomString(8)}`,
            trade_name: `Trade Name ${generateRandomString(8)}`,
            tax_id: Math.floor(10000000000 + Math.random() * 90000000000).toString(),
            email_address: `provider_${generateRandomString(6)}@example.com`,
            office_phone: `021${Math.floor(10000000 + Math.random() * 90000000)}`,
            mailing_address: `Tehran, Street ${generateRandomString(5)}`
        });
        
        providerRequests.push({
            method: 'POST',
            url: `${BASE_URL}/providers`,
            body: payload,
            params: { 
                headers: { 'Content-Type': 'application/json' },
                tags: { name: 'SetupCreateProvider' } 
            }
        });
    }

    const responses = http.batch(providerRequests);
    const providerIds = [];

    responses.forEach((res, index) => {
        if (res.status === 201) {
            try {
                const body = res.json();
                providerIds.push(body.provider_id);
                console.log(`Provider: ${body.provider_id} Created.`);
            } catch (e) {
                console.error(`Failed to parse provider response ${index}: ${res.body}`);
            }
        } else {
            // Core provider (index 0) usually fails if uniqueness constraints hit, which is normal
            console.error(`Failed to create provider ${index}. Status: ${res.status}`);
        }
    });

    if (providerIds.length === 0) {
        throw new Error("Setup failed: No providers were successfully created.");
    }

    console.log(`Setup Complete: Created ${providerIds.length} Providers.`);
    return { providerIds }; 
}

// 2. Main VU Logic: Execute the core workflow per iteration
export default function (data) {
    const allProviders = data.providerIds;
    
    const numProvidersToSelect = Math.floor(Math.random() * Math.min(7, allProviders.length - 1)) + 2; 
    const shuffledProviders = shuffleArray([...allProviders]);
    const selectedProviders = shuffledProviders.slice(0, numProvidersToSelect);
    
    const nid = Math.floor(1000000000 + Math.random() * 9000000000).toString(); 
    let userId = null;

    const priorityList = [];
    const baseAmount = 50000;

    
    for (let i = 0; i < selectedProviders.length; i++) {
        const currentProviderId = selectedProviders[i];
        
        // 1. Link a user to a provider
        const userPayload = JSON.stringify({
            external_metadata: "string",
            internal_metadata: "string",
            nid: nid,
            provider_id: currentProviderId
        });

        let userRes = http.post(`${BASE_URL}/users`, userPayload, {
            headers: { 'Content-Type': 'application/json' },
            tags: { name: 'CreateUser' }
        });

        const body = userRes.json();
        userId = body.user_id
        
        // 2. Do Credit
        const creditPayload = JSON.stringify({
            amount: baseAmount,
            description: "string",
            idempotency_key: uuidv4()
        });
        
        const creditRes = http.post(`${BASE_URL}/transactions/${currentProviderId}/users/${userId}/credit`, creditPayload, {
            headers: { 
                'Content-Type': 'application/json',
                'Idempotency-Key': uuidv4()
            },
            tags: { name: 'AddCredit' }
        });

        const creditSuccess = check(creditRes, { 'credit added': (r) => r.status === 200 || r.status === 201 });
        if (!creditSuccess && __ITER < 1) {
            console.error(`Add credit failed for provider ${currentProviderId}. Status: ${creditRes.status} Body: ${creditRes.body}`);
        }

        // 3. Add to Priority List
        priorityList.push({
            provider_id: currentProviderId,
            max_amount: baseAmount,
            usage_type: (i % 2 === 0) ? "MultiUse" : "SingleUse"
        });
    }
    
    // --- Create Priority ---
    const expireTime = new Date(new Date().getTime() + 60 * 60 * 1000).toISOString().split('.')[0] + 'Z';
    const priorityPayload = JSON.stringify({
        expires_at: expireTime,
        priorities: priorityList
    });

    let res = http.post(`${BASE_URL}/users/${userId}/priorities`, priorityPayload, {
        headers: { 
            'Content-Type': 'application/json',
            'Idempotency-Key': uuidv4()
        },
        tags: { name: 'CreatePriority' }
    });

    const prioritySuccess = check(res, { 'priority created': (r) => r.status === 200 || r.status === 201 });
    if (!prioritySuccess && __ITER < 1) {
        console.error(`Priority failed. Status: ${res.status} Body: ${res.body}`);
    }
    
    // --- Randomly choose between Debit OR Cancel ---
    const isDebit = Math.random() < 0.5;

    if (isDebit) {
        // --- Debit ---
        const debitPayload = JSON.stringify({
            idempotency_key: uuidv4(),
            amount: baseAmount - 10000, 
            description: "Test consume"
        });

        const debitRes = http.post(`${BASE_URL}/transactions/${selectedProviders[0]}/users/${userId}/debit`, debitPayload, {
            headers: { 
                'Content-Type': 'application/json'
            },
            tags: { name: 'ConsumeCredit' }
        });
        
        const debitSuccess = check(debitRes, { 'debit successful': (r) => r.status === 200 || r.status === 201 });
        if (!debitSuccess && __ITER < 1) {
            console.error(`Debit failed. Status: ${debitRes.status} Body: ${debitRes.body}`);
        }
    } else {
        // --- Cancel Active Priority ---
        const cancelRes = http.del(`${BASE_URL}/users/${userId}/priorities/active`, null, {
            headers: { 
                'Accept': 'application/json'
            },
            tags: { name: 'CancelPriority' }
        });
        
        const cancelSuccess = check(cancelRes, { 'priority cancelled': (r) => r.status === 200 || r.status === 204 });
        if (!cancelSuccess && __ITER < 1) {
            console.error(`Cancel failed. Status: ${cancelRes.status} Body: ${cancelRes.body}`);
        }
    }
    
    //sleep(1);
}
