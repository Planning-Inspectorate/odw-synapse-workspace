const { ServiceBusClient } = require("@azure/service-bus");

const connectionString = "<SERVICE BUS NAMESPACE CONNECTION STRING>"
const topicName = "<TOPIC NAME>";

const messages = [
	{ body: "{\"Profession\":\"Petroleum engineer\",\"Pers_No\":\"10000001\",\"Employee_Number\":\"15433721\",\"First_Name\":\"Emma\",\"Last_Name\":\"Reed\",\"PA\":\"P500\",\"Personnel_Area\":\"Strategy\",\"Personnel_SubArea\":\"Human Resources\",\"PSubArea\":\"P615\",\"Org_unit\":\"44751217\",\"Organizational_Unit\":\"Change Portfolio\",\"Organizational_Key\":\"P2000000090530\",\"WorkC\":\"PE\",\"Work_Contract\":\"Permanent\",\"CT\":\"01\",\"Contract_Type\":\"Permanent Contract\",\"PS_Group\":\"AA\",\"Pay_Band_Description\":\"Higher Executive Officer\",\"FTE\":\"92\",\"Wk_Hrs\":\"33\",\"Is_Part_Time\":\"False\",\"S\":\"3\",\"Employment_Status\":\"Active\",\"Gender_Key\":\"Female\",\"TRA_Start_Date\":\"04-27-2010\",\"TRA_End_Date\":\"03-19-2016\",\"Prev_PersNo\":\"475441\",\"ActR\":\"06\",\"Reason_For_Action\":\"Restructure\",\"Position\":\"88300211\",\"Position1\":\"Engineering geologist\",\"Cost_Ctr\":\"76150\",\"Cost_Centre\":\"Inspectr Costs\",\"Civil_Service_Start\":\"1979-06-24T16:00:13\",\"Date_To_Current_Job\":\"03-13-2021\",\"Seniority_Date\":\"08-05-2020\",\"Date_To_Subst_Grade\":\"2021-06-07T14:46:20\",\"Pers_No_1\":\"22727969\",\"Name_of_Manager\":\"Hayley Scott-Kent\",\"Manager_Position\":\"90785928\",\"Manager_Position_Text\":\"Data processing manager\",\"Counter_Sign_Manager\":\"Adam Elliott-Sutton\",\"Loc\":\"003\",\"Location\":\"Bristol\",\"Org_Start_Date\":\"2016-07-18T12:18:29\",\"Fix_Term_End_Date\":\"05-27-2021\",\"Loan_Start_Date\":\"01-03-2022\",\"Loan_End_Date\":\"06-05-2020\",\"EEGrp\":\"1\",\"Employee_Group\":\"Employee\",\"Annual_salary\":\"xxxx\",\"Curr\":\"GBP\",\"Birth_date\":\"04-10-1996\",\"PArea\":\"PI\",\"Payroll_Area\":\"PINS\",\"Company_Code\":\"68273557\",\"CoCd\":\"52439202\",\"Email_Address\":\"Emma.Reed@planninginspectorate.gov.uk\",\"Address_Line_1\":\"387 Taylor green\",\"Town\":\"Port Lorrainetown\",\"County\":\"East Sussex\",\"Postcode\":\"WC6Y 9UP\",\"Phone_Number\":\"(07700) 900 513\",\"Annual_Leave_Start\":\"12-10-2020\",\"Active_Status\":\"NON-ACTIVE\",\"Is_Charting_Officer\":\"True\",\"Charting_Officer_ID\":\"29125273\",\"Grade\":\"NSI\",\"Is_Sub_Group_Leader\":\"False\",\"Is_APO\":\"False\",\"Is_APOM\":\"True\",\"FTE_Primary\":\"65\",\"FTE_Secondary\":\"80\",\"Location_Primary\":\"Home\",\"Location_Secondary\":\"Bristol\",\"Leaving_Date\":\"2023-02-17T10:45:27\"}" }
 ];

 async function main() {
	// create a Service Bus client using the connection string to the Service Bus namespace
	const sbClient = new ServiceBusClient(connectionString);

	// createSender() can also be used to create a sender for a queue.
	const sender = sbClient.createSender(topicName);

	try {
		// Tries to send all messages in a single batch.
		// Will fail if the messages cannot fit in a batch.
		// await sender.sendMessages(messages);

		// create a batch object
		let batch = await sender.createMessageBatch(); 
		for (let i = 0; i < messages.length; i++) {
			// for each message in the arry			

			// try to add the message to the batch
			if (!batch.tryAddMessage(messages[i])) {			
				// if it fails to add the message to the current batch
				// send the current batch as it is full
				await sender.sendMessages(batch);

				// then, create a new batch 
				batch = await sender.createMessageBatch();

				// now, add the message failed to be added to the previous batch to this batch
				if (!batch.tryAddMessage(messages[i])) {
					// if it still can't be added to the batch, the message is probably too big to fit in a batch
					throw new Error("Message too big to fit in a batch");
				}
			}
		}

		// Send the last created batch of messages to the topic
		await sender.sendMessages(batch);

		console.log(`Sent a batch of messages to the topic: ${topicName}`);

		// Close the sender
		await sender.close();
	} finally {
		await sbClient.close();
	}
}

// call the main function
main().catch((err) => {
	console.log("Error occurred: ", err);
	process.exit(1);
 });