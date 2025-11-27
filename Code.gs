// Main function to filter company data
function filterCompanies() {
  const lock = LockService.getScriptLock();
  try {
    Logger.log("Companies filter started ...");
    if (!lock.tryLock(300000)) {
      updateScriptStatus("Script already running -- Skipping!", true);
      return false;
    }
    updateScriptStatus(`Companies filter started ...`, true, true);

    // Setup and Initial Checks
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inputSheet = ss.getSheetByName("Input Companies");
    const outputSheet = ss.getSheetByName("New Companies");
    const masterSheet = ss.getSheetByName("Master");

    if (!inputSheet || !outputSheet || !masterSheet) {
      updateScriptStatus("One or more sheets are missing.", true);
      return false;
    }

    // Data Preprocessing
    if (inputSheet.getLastRow() <= 1) {
      updateScriptStatus("No new input data found.", true);
      return;
    }

    const inputData = inputSheet.getRange(2, 1, inputSheet.getLastRow() - 1, inputSheet.getLastColumn()).getValues();
    const masterData = masterSheet.getDataRange().getValues();
    updateScriptStatus(`Processing ${inputData.length} Companies.`, true);

    let filteredData = companyInputsPrimaryFiltering(inputData, masterData);
    if (filteredData.length === 0) {
      updateScriptStatus(`All ${inputData.length} companies are already in Master sheet.`, true);
      return;
    }

    updateScriptStatus(`Found ${filteredData.length} new valid companies.`, true);
    filteredData = filteredData.slice(0, MAX_PROCESSED_AT_RUN);

    // Data Processing
    const updatedData = filteredData.map(row => getRestructuredRow(row));
    const rewrittenDescriptions = processDescriptionsInBatches(updatedData.map(row => row[2]));
    const adjustedIndustries = processIndustriesInBatches(updatedData.map(row => row[5]));
    const rewrittenData = updatedData.map((row, index) => {
      row[2] = rewrittenDescriptions[index];
      row[5] = adjustedIndustries[index];
      return row;
    });

    if (!rewrittenData || rewrittenData.length === 0) {
      updateScriptStatus("No new valid company found after filtering.", true);
      return false;
    }

    // Outputting Results
    const inputHeaders = ["company_id", "company_name", "company_description", "ai_company_description", "company_year", "company_location", "company_industry", "company_specialties", "company_size", "company_website", "company_logo", "company_url", "company_jobs"];
    const outputHeaders = ["company_id", "company_name", "company_description", "company_year", "company_location", "company_industry", "company_size", "company_website", "company_logo", "first_name", "last_name", "hr_email", "info_email", "support_email"];

    // Batch write to output sheet
    if (outputSheet.getDataRange().isBlank()) {
      outputSheet.appendRow(outputHeaders);
    }
    outputSheet.getRange(outputSheet.getLastRow() + 1, 1, rewrittenData.length, rewrittenData[0].length).setValues(rewrittenData);

    // Batch write to master sheet
    if (masterSheet.getDataRange().isBlank()) {
      masterSheet.appendRow(inputHeaders);
    }
    masterSheet.getRange(masterSheet.getLastRow() + 1, 1, filteredData.length, filteredData[0].length).setValues(filteredData);

    // Final Status Update
    updateScriptStatus(`${rewrittenData.length} new companies added.`, true);
    ss.toast(`${inputData.length} jobs processed.\n${rewrittenData.length} new companies added.\nNew run scheduled every 5 min.`, 'Processing Complete', 10);

    ensureTriggerIsActive();
    return true;

  } catch (error) {
    console.error("Unexpected error: ", error.stack);
    updateScriptStatus(`Unexpected error: ${error}`, true);
    return false;
  } finally {
    lock.releaseLock();
    Logger.log("Lock released.");
    setScriptStopped()
  }
}


// Functions for rewriting descriptions and industries
function rewriteCompaniesDescriptions(companyDescriptions) {
  Logger.log("Rewriting descriptions in batches");

  const maxRetries = 5;
  let delay = 1000; // Initial delay for exponential backoff

  function generateRequests(descriptions) {
    return descriptions.map(desc => {
      const requestBody = {
        model: MODEL_TYPE,
        messages: [
          { role: 'system', content: 'You are a helpful assistant.' },
          {
            role: 'user',
            content: `Rewrite the company description below to be unique, engaging, concise (under ${MAX_TOKENS - 50} tokens), and professional. Omit any unknown details rather than using placeholders, and ensure it's ready for direct database use without further editing. If the provided company description is empty, contains only placeholders, or cannot be rewritten for any reason, return an empty string (""). Do not provide explanations or alternative responses.

            Original company description:
            ${desc}`
          }
        ],
        max_tokens: MAX_TOKENS,
      };

      return {
        url: 'https://api.openai.com/v1/chat/completions',
        method: 'post',
        headers: {
          'Authorization': `Bearer ${API_KEY}`,
          'Content-Type': 'application/json'
        },
        payload: JSON.stringify(requestBody),
        muteHttpExceptions: true
      };
    });
  }

  let attempts = 0;
  while (attempts < maxRetries) {
    const requests = generateRequests(companyDescriptions);
    const responses = UrlFetchApp.fetchAll(requests);

    let rateLimited = false;
    const results = [];

    for (let i = 0; i < responses.length; i++) {
      const resp = responses[i];
      const code = resp.getResponseCode();

      if (code === 429) {
        rateLimited = true;
        break;
      } else if (code >= 200 && code < 300) {
        const json = JSON.parse(resp.getContentText());
        let rewritten = json.choices && json.choices[0].message.content.trim();
        const helper = rewritten.replace("?", "").trim();
        if (rewritten === '""' || !rewritten || helper.length < 10) {
          rewritten = "";
        }
        results.push(rewritten);
      } else {
        Logger.log(`Error response code: ${code} ${resp.getContentText()}`);
        results.push(companyDescriptions[i]); // Fallback to original
      }
    }

    if (!rateLimited) {
      return results;
    } else {
      attempts++;
      Logger.log(`Rate limit exceeded, retrying... attempt ${attempts}`);
      Utilities.sleep(delay);
      delay *= 2;
    }
  }
  return companyDescriptions; // Return originals after all retries fail
}



function processDescriptionsInBatches(companyDescriptions) {
  updateScriptStatus(`Rewriting ${companyDescriptions.length} company descriptions...`);

  const queue = [];
  for (let i = 0; i < companyDescriptions.length; i += MAX_BATCH_SIZE) {
    queue.push(companyDescriptions.slice(i, i + MAX_BATCH_SIZE));
  }

  const results = [];
  while (queue.length > 0) {
    Logger.log(`Remaining batches in queue: ${queue.length}`);
    const batch = queue.shift();
    const rewrittenBatch = rewriteCompaniesDescriptions(batch);
    results.push(...rewrittenBatch);
    updateScriptStatus(`Completed ${results.length} company descriptions!`);
  }

  Logger.log(`Final results: ${results.length}`);
  return results.map((desc, index) => {
    const originalDesc = companyDescriptions[index];
    return (typeof originalDesc === 'string' && originalDesc.trim() === "") ? "" : desc;
  });
}



function rewriteCompanyIndustries(companyIndustries) {
  Logger.log("Rewriting industries in batches");

  const maxRetries = 5;
  let delay = 1000;

  function generateRequests(industries) {
    return industries.map(inds => {
      const requestBody = {
        model: MODEL_TYPE,
        messages: [
          { role: 'system', content: 'You are a helpful assistant.' },
          {
            role: 'user',
            content: `Change the following company industry to match the valid domains, separating multiple matches with commas. If no match is found, set "General".\n
            ***IMPORTANT: Output ONLY the edited company industry. Do NOT include the original company industry, and do NOT add any explanations or comments.***\n\n
            Valid Domains:\n${COMPANY_DOMAINS.join(", ")}\n\nCompany Industry:\n${inds}`
          }
        ],
        max_tokens: 50,
      };

      return {
        url: 'https://api.openai.com/v1/chat/completions',
        method: 'post',
        headers: {
          'Authorization': `Bearer ${API_KEY}`,
          'Content-Type': 'application/json'
        },
        payload: JSON.stringify(requestBody),
        muteHttpExceptions: true
      };
    });
  }

  let attempts = 0;
  while (attempts < maxRetries) {
    const requests = generateRequests(companyIndustries);
    const responses = UrlFetchApp.fetchAll(requests);

    let rateLimited = false;
    const results = [];

    for (let i = 0; i < responses.length; i++) {
      const resp = responses[i];
      const code = resp.getResponseCode();

      if (code === 429) {
        rateLimited = true;
        break;
      } else if (code >= 200 && code < 300) {
        const json = JSON.parse(resp.getContentText());
        const rewritten = json.choices && json.choices[0].message.content.trim();
        results.push(rewritten);
      } else {
        Logger.log(`Error response code: ${code} ${resp.getContentText()}`);
        results.push(companyIndustries[i]);
      }
    }

    if (!rateLimited) {
      return results;
    } else {
      attempts++;
      Logger.log(`Rate limit exceeded, retrying... attempt ${attempts}`);
      Utilities.sleep(delay);
      delay *= 2;
    }
  }
  return companyIndustries;
}

function processIndustriesInBatches(companyIndustries) {
  updateScriptStatus(`Adjusting ${companyIndustries.length} company industries...`);

  const queue = [];
  for (let i = 0; i < companyIndustries.length; i += MAX_BATCH_SIZE) {
    queue.push(companyIndustries.slice(i, i + MAX_BATCH_SIZE));
  }

  const results = [];
  while (queue.length > 0) {
    Logger.log(`Remaining batches in queue: ${queue.length}`);
    const batch = queue.shift();
    const rewrittenBatch = rewriteCompanyIndustries(batch);
    results.push(...rewrittenBatch);
    updateScriptStatus(`Completed ${results.length} company industries!`);
  }

  Logger.log(`Final results: ${results.length}`);
  return results;
}


/**
 * This function restructure the given row.
 * @param {Object[]} input_row - A table row parameter.
 */
function getRestructuredRow(row) {
  // adjust company location
  let company_location = row[5];
  const location_parts = company_location.split(",");
  if (location_parts && location_parts.length > 0) {
    company_location = location_parts[location_parts.length - 1].trim();
  }

  // set the new columns first_name, last_name, hr_email, info_email, support_email
  // first and last name:
  let first_name = row[1];
  // if (typeof first_name !== 'string') {
  //   first_name = String(first_name || '');
  // }
  // first_name = first_name.replace(/[?!{}]/g, ''); // remove weird characters
  // first_name = capitalizeWords(first_name); // capitalize

  const last_name = company_location;

  // adjust company industry
  let company_industry = row[6].replace(" . ", ", ");
  if (company_industry == "—" || company_industry.length < 2) {
    company_industry = "General"
  }

  // setting the emails:
  const domain = getValidDomain(row[9]).toLowerCase()
  let hr_email = ""
  let info_email = ""
  let support_email = ""

  if (domain && domain.toLowerCase().includes("gmail.com")) {
    hr_email = domain;
    info_email = domain;
    support_email = domain;

  } else if (domain) {
    hr_email = "hr@" + domain;
    info_email = "info@" + domain;
    support_email = "support@" + domain;
  }

  const new_r = [row[0], first_name, row[2]].concat([row[4], company_location, company_industry]).concat(row.slice(8, 11)).concat([first_name, last_name, hr_email, info_email, support_email])

  return new_r
}

/**
 * This function fliter out unnecessary columns from provided inputData table, and return the new rows compared to other masterData table.
 * @param {Object[][]} inputData - A table parameter.
 * @param {Object[][]} masterData - A table parameter.
 */
function companyInputsPrimaryFiltering(inputData, masterData) {
  // filter out unnecessary data

  // First adjusting names and websites
  const adjusted_data = inputData.map(row=> {
    // Adjusting names
    let company_name = row[1];
    if (typeof company_name !== 'string') {
      company_name = String(company_name || '');
    }
    company_name = company_name.replace(/[?!{}]/g, '').trim(); // remove weird characters
    company_name = capitalizeWords(company_name); // capitalize
    row[1] = company_name;

    // adjusting website:
    let company_website = row[9];
    const domain = getValidDomain(company_website).toLowerCase();
    if(domain) {
      company_website = company_website.trim().toLowerCase();
    }else {
      company_website = "";
    }
    row[9] = company_website;

    return row;
  
  })

  // Filter out companies with empty company name and websites
  const filteredData = inputData.filter(row => {
    // Ensure company_name is not empty, undefined, or the special character "—"
    var company_name = row[1] && row[1].toString().trim();  // Handle null or undefined
    if (!company_name || company_name === "—") {
      return false;
    }
    return true;
  });

  // filtering out invalid websites
  const validData = filteredData.filter(row => {
    const excluded_domains = ["facebook.com", "instagram.com", "linkedin.com"];
    const website = row[9];
    
    if(!website) {
      return false;
    }
    
    let is_valid = true
    excluded_domains.forEach(domain => {
      if (website.includes(domain)) {
        is_valid = false;
      }
    })
    return is_valid;
  });

  // Compare with Master sheet and filter out duplicates
  const masterComapayIds = masterData.map(row => row[0]);

  return validData.filter(row => {
    return !masterComapayIds.includes(row[0]);
  });
}


function getValidDomain(company_website) {
  let domain = ""

  if (company_website == "#ERROR!" || company_website == "Company website is empty") {
    return ""
  }

  if (company_website.trim().split(" ").length > 1) {
    return ""
  }

  const gmail_match = company_website.match(/\b[A-Za-z0-9._%+-]+@gmail\.com\b/i);

  if (gmail_match && gmail_match.length > 0) {
    return gmail_match[0];
  }

  if (company_website.includes("@")) {
    domain = company_website.split("@")
    return domain[domain.length - 1]
  }

  const match = company_website.match(/(?:https?:\/\/)?(?:www\.)?([^\/]+)/i);

  if (match) {
    domain = match[1];  // Extracted domain
  }

  if (domain.length < 3) {
    return ""
  }

  return domain
}

function capitalizeWords(input) {
  return input.replace(/\b\w/g, (char) => char.toUpperCase());
}

// Helper functions for consolidated status updates
function updateScriptStatus(message, log = false, change_last_updated = false) {
  const scriptProperties = PropertiesService.getScriptProperties();
  const timestamp = new Date().toLocaleTimeString();
  const currentProgress = scriptProperties.getProperty("progress_state") || "";
  var newProgress = `${currentProgress}\n - ${message}`;
  if (change_last_updated) {
      newProgress = `- ${message}`;
  }

  scriptProperties.setProperties({
    "progress_state": newProgress,
    "is_script_running": "true"
  });

  if (change_last_updated) {
    scriptProperties.setProperty("last_updated", timestamp);
  }

  if (log) {
    console.log(message);
  }
}

function showSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('Sidebar')
    .setWidth(300)
    .setHeight(150)
    .setTitle('Company Processing Progress');
  SpreadsheetApp.getUi().showSidebar(html);
}

function scheduleTrigger() {
  // Run filterJobs function every 5 minutes
  ScriptApp.newTrigger('filterCompanies')
    .timeBased()
    .everyMinutes(5)
    .create();
}

function ensureTriggerIsActive() {
  const triggerFunctionName = "filterCompanies";
  const existingTriggers = ScriptApp.getProjectTriggers();
  let triggerExists = false;

  // Check if the trigger already exists
  existingTriggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === triggerFunctionName) {
      triggerExists = true;
    }
  });

  // If the trigger does not exist, create it
  if (!triggerExists) {
    scheduleTrigger();
    updateScriptStatus("Trigger created successfully.")
    Logger.log('Trigger created successfully.');
  } else {
    Logger.log("Trigger is already active")
  }
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu("Custom tools")
  menu.addItem("Filter new companies", "filterCompanies").addToUi();
  menu.addItem("Show sidebar", "showSidebar").addToUi();

  showSidebar();
}

function setScriptRunningStatusProperty(is_script_running="true") {
  PropertiesService.getScriptProperties().setProperty('is_script_running', is_script_running);
}

function setScriptStopped() {
    PropertiesService.getScriptProperties().setProperty('is_script_running', "false");
}

function getProgressStatus() {
  var scriptProperties = PropertiesService.getScriptProperties();
  return {
    isScriptRunning: scriptProperties.getProperty('is_script_running'),
    progressState: scriptProperties.getProperty('progress_state'),
    lastUpdated: scriptProperties.getProperty('last_updated')
  };
}

