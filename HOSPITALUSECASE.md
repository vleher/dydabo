## Hospital Use Case

### Requirements 

This is a use case that I got from [this blog post](https://sumitpal.wordpress.com/2016/07/03/hbase-schema-design-example/) and it seems to depict a real application scenario. 

A Patient has their own attributes with demographic info – first name, last name, address(es), phone number(s), date of birth etc. When a patient goes to the doctor, it is called as an encounter. An encounter can result in the patient having multiple procedures, medications, diagnosis, patient notes and so on. 

* 1 Patient can have multiple encounters and each of them has an unique encounter id.
* 1 Encounter can have multiple procedures, medication, diagnosis, patient notes (each of these have a unique id)
* 1 Encounter can have multiple claims each with an unique id
* 1 Claim can have multiple claim details each with an unique id
* 1 Claim can also have multiple claim charges each with unique id 
* each of the diagnosis, medication, procedure etc also have an unique code associated with it. 

### Design and Implementation

Unlike relational databases, keeping multiple copies of the same data for easy access is not a bad thing in NoSql databases. We will start by creating POJO or data beans in the application such that these information can be easily accessed and used by the application.

First thing to consider is the different ways we will need to access the data from the back end. These are probably the type of queries that the application will use to get data from the database.

1. All the patients in the system
1. Get a patient by patient id
1. All the patients with a specific first name or a last name or both.
1. All the patients with a specific medication (we will ignore diagnosis and procedures in this example, as it will be similar to medication. just to keep it simple)
1. All encounters for a given patient id
1. All encounters for a patient given the first name, or last name or both
1. All encounters with a specific medication id or code (all patients)
1. All encounters with a specific medication id for a patient name with a specific first and last name (or patient id)
1. All medications for a given patient id
1. Get a claim by claim id
1. All claims for a specific patient id (or by last name)
1. All claim charges for a specific patient id

There are several more queries you can come up with but most of them will be some kind of a variation of the above. We will also assume the following in this use case:

* after an encounter the data will not change
* any subsequent change will be the result of another encounter

Now, we will need to design the data objects such that the data can easily be used by the application. The design below is probably just one of the many you can come up with. It is more geared towards all the different ways the library can be used to save and access data. Your own design will probably differ depending on your requirements.

So, first create a class to hold the patient details

```
public class Patient implements BlackBoxable {

    private String pId = null; // patient id
    private String fN = null; // first name
    private String lN = null; // last name
    private Date dob = null; // date of birth
    // a map of addresses such as Home, Work etc
    private Map<String, Address> address = new HashMap<>(); 
    private List<String> em = new ArrayList<>(); // a list of email addresses

    public Patient() {
    }
    
    public Patient(String id, String firstName, String lastName) {
        this.pId = id;
        this.fN = firstName;
        this.lN = lastName;
    }
 }
```

> **TIP**: You should probably use short variable names as these will be used as column names. A long variable name, while useful for legibility in Java classes can create large disk footprints for the table. You can choose something that works for both and keep your variable length to 3 to 5 characters.

> **TIP**: The complete path of the class is used to create database tables. Try to keep the package names short such as *com.domain.db* which will result in shorter table names. Just something to consider.


The next step is to consider what you would want to use as the row key for patients. Assume that we will ***almost*** always have the patient id to look up the patients. In that case the row key should be the patient id. This will be the simplest case.

```
@Override
public String getBBRowKey() {
    return getpId();
}    
```
Now, let us assume that we will also need to look up the user by first name, last name in addition to the patient id. In that case it probably makes sense to keep those fields in the row key as well. The following is an example of such a key which has the id, first name and last name, delimited by a colon. 

```
@Override
public String getBBRowKey() {
    return getpId() + ":" + getfN() + ":" + getlN();
}
```

Before we go ahead with other classes, let quickly take a look at the class that is referenced from the **Patient** class, such as **Address**. We have a Map of Addresses for each patient. 

````
class Address {
    private String street;
    private String city;
    private String state;
    private String country;

    public Address(String street, String city, String state, String country) {
        this.street = street;
        this.city = city;
        this.state = state;
        this.country = country;
    }
    ....
}
````

The class should be self explanatory. Notice that we do not have to implement the **BlackBoxable** interface as this class will not have its own table. The data will be stored as part of the Patient table. 

> **TIP**: A **Map** inside an object will be saved as a separate column family in the table. So, the Address above will be a column family which will have columns named as the keys of the Map. So, it could be Home, Work, Other etc in this case. Use Map objects wisely if you do not want a lot of column families.

You must have noticed that the emphasis is on the application here and much less the database design. Although you could argue that it closely relates to a relational model at this time. Start with how best to use the data in your application first, we can fine tune the database later.

Let's go ahead and build our other classes that relates to Medication, Claim, ClaimDetails and ClaimCharges etc. The Procedure and Diagnosis classes will closely resemble the Medication, so we will omit it for simplicity.

```
public class Medication implements BlackBoxable {

    private String mId = null; // medication id
    private String mCode = null; // medication code
    private String mName = null; // med name
    private Integer mDose = null; // a typical med dosage (not patient data)
    private String mVendor = null; // med vendor 

    public Medication() {
    }

    public Medication(String mId) {
        this.mId = mId;
        this.mName = mId;
    }
    ....
    
    @Override
    public String getBBRowKey() {
    	//assumption: we will probably look up the medication by its name rather than by its id.
        return getmName();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }
}
```

The **Claim** class keeps a reference to the patient id, which will allow us to quickly look up based on patient ids rather than having to scan all encounters. More on that later....

```
public class Claim implements BlackBoxable {

    private String cId = null; // claim id
    private String pId = null; // patient id
    private List<ClaimDetails> cDets = null; // list of claim details associated with this claim
    private List<ClaimCharges> cCharges = null; // list of claim charges associated with this claim

    public Claim(String cId, String pId) {
        cDets = new LinkedList<>();
        cCharges = new LinkedList<>();
        this.cId = cId;
        this.pId = pId;
    }
    .............
    @Override
    public String getBBRowKey() {
        return getcId();
    }    
}
```

You now have the choice of keeping the ClaimDetails and ClaimCharges as separate tables or just as part of the claim. Either will work and what you want to do will depend on the access requirements. For example,  if you want to very quickly lookup claim charges with out parsing through all the claims, then keeping it as a separate table might help.

```
public class ClaimDetails implements BlackBoxable {

    private String cdId = null;
    private String cdNotes = null;

    public ClaimDetails(String cdId) {
        this.cdId = cdId;
    }
```

```
public class ClaimCharges implements BlackBoxable {
    public enum TranType { CREDIT, DEBIT }

    private String ccId = null; // claim charge id
    private Date ccDate = null; // claim date
    private TranType type = null; // transaction type
    private Double amount = 0.0;` // transaction amount
    ....... 
    @Override
    public String getBBRowKey() {
        return getCcId();
    }    
}
```


A sample implementation of **Encounter** object, which is probably the crux of the entire application as it will hold the most of the data and the relationships between all the data. 

```
public class Encounter implements BlackBoxable {

    private String pId = null; // patient id
    private String eId = null; //encounter id 
    private String pNotes = null; // patient notes from this encounter
    private List<Procedure> procs = new LinkedList<>(); // list of procedures
    private List<Medication> meds = new LinkedList<>(); // list of medications
    private List<Diagnosis> diags = new LinkedList<>(); // list of diagnosis
    private List<Claim> cls = new LinkedList<>(); // list of claims

    // Selective Duplicate Data for easy access and search (optional)
    private String pFN = null; // patient first name
    private String pLN = null; // patient last name
	// a comma separated list of all medications and diagnosis for quick search
    private String medIds = ""; 
    private String diagIds = "";

    // Complete Duplicate Data of the patient so that we don't have to query the patient table
    private Patient patient = null; // a complete patient information object
    
    public Encounter() {
    }

    public Encounter(String eId, String pId) {
        this.eId = eId;
        this.pId = pId;
    }

    public Encounter(String eId, String pId, String pFN, String pLN) {
        this(eId, pId);
        this.pFN = pFN;
        this.pLN = pLN;
    }

    public Encounter(String eId, Patient pat) {
        this(eId, pat.getpId());
        this.patient = pat;
    }

	......
	
	@Override
    public String getBBRowKey() {
    	// assumption: we will probably look up encounter based on patient id more often than encounter id.
    	// keeping the row keys sorted by patient id is probably more efficient.
        return getpId() + ":" + geteId();
    }    
}
```

Now that we have all the data mapped out, let's see how we can store and access these data quickly using the library. It is as simple as creating all the objects with the appropriate data in each of them.

Let's create a new **Patient** and new **Encounter** for the patient in the simplest way possible.

```
Patient patient = new Patient("12345P", "David", "Warner");

Encounter enc = new Encounter("98765E", patient);
enc.setpFN(patient.getfN()); // set first name of the patient
enc.setpLN(patient.getlN()); // set the last name of the patient
// add some medications, procedures etc to the list
enc.addDiagnosis(diagnosis);
enc.addProcedures(procedure);
enc.addMedication(medication);
// add a claim
Claim claim = new Claim("3434CL");
enc.addClaim(claim);
.....

// update or save the data to the back end
BlackBox blackBox = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);

// save the patient and the encounter
if (blackBox.update(patient)) {
	blackBox.insert(enc); // you could use update as well here.
}
// you could save the claim separately if needed
blackBox.insert(claim);
```

> **TIP**: Remember that there is no support atomic transaction in the NoSql database. That means you will have to factor in the possibility of an update failing (or inconsistent duplicate data)...either at the time of saving or when fetching the data.

Once the data has been saved to the tables, you will need to access them in various different ways. We will go through the scenarios that we detailed earlier and see how each of them could be implemented. 

#### all patients in the system

If you have millions of patients, then this is probably not a great idea. But you could do it if you wanted to....

```
// create a bean with all null or empty values and pass it to search for all patients
<Patient> allPatients = blackBox.search(new Patient());
```

#### patient by patient id

Assume that the key on the patient table is of the format *patient Id:first name:last name*. 

```
// we will generate a regular expression based on the patient id 
String pIdKey = "12345P:.*";
List<Patient> pList2 = blackBox.fetchByPartialKey(pIDKey, new Patient());
```

If you used just your patient id as the row key on the table, then it is much faster to look up. 

```
String pId = "12345P";
List<Patient> pList = blackBox.fetch(pId, new Patient());

// also can lookup multiple patients together
List<String> pIds = Arrays.asList("12345P", "7777P", "23423P");
List<Patient> pList = blackBox.fetch(pIds, new Patient());
```

#### patients by name

So, you have to look up a patient with either the first or the last name. You can do it in either of the two ways depending on if you have used the name as part of the row key. You can mix and match to search for any fields.

```
// All Patients with a specific first name (column value search)
Patient p1 = new Patient();
p1.setfN("Tina");
Patient p2 = new Patient();
p2.setfN("Diana");
Patient p3 = new Patient();
p3.setfN("X.*")
// all patients with either the first name Tina or the name Diana or if the name starts with an X.
List<Patient> pList3 = blackBox.search(Arrays.asList(p1, p2, p3));

// match either first name Tina or last name Jackson
Patient p1 = new Patient();
p1.setfN("Tina");
Patient p2 = new Patient();
p2.setlN("Jackson");
List<Patient> pList3 = blackBox.search(Arrays.asList(p1, p2));

// All Patients with first name 'Diana' OR last 'Turner' (assuming row key search)
List<String> queryKeys = Arrays.asList(".*:Diana:.*", ".*:.*:Turner");
List<Patient> pList5 = blackBox.fetchByPartialKey(queryKeys, p);

// All Patients with specific first name (row key search)
String fNameKey = ".*:Cyndi:.*";  // regular expression match against the row key 
List<Patient> pList4 = blackBox.fetchByPartialKey(fNameKey, p);
```

> **TIP**: Use fetch(...) if you know the exact row key, which will be the fastest. Use fetchByPartialKey(...) if you know only part of the row key. Use search(...) if you don't know the key and want to do a column value match.

#### patients with specific medication

In the data model, we kept the medication names as a first level comma separated list for easier search. So, just as in the earlier example we can do a regular expression search on that field.

```
String diagId = "34532M"; 

Encounter pe = new Encounter();
pe.setMedIds(".*" + diagId + ".*"); // create a regex to match against the string
List<Encounter> peList = blackBox.search(pe); // list of all encounters that have the patient info in them.

// if there is no string of medications at the top level, we can also do a deeper search inside 
// the List of medication to find a match, albeit slower
Encounter pe1 = new Encounter(); // an encounter object
Medication d = new Medication(diagId); // the medication object to search for
List<Medication> diags = new ArrayList<>(); 
diags.add(d); // add the medication object to the list of meds
pe1.setDiags(diags); // set that to the encounter

// we now have an object with just the medication we want to search for.
List<Encounter> peList1 = blackBox.search(pe1);
for (Encounter enc: peList1) {
	System.out.println(" Patient :" + enc.getPatient()); // this might have duplicates
}
```
> **TIP**: The general rule is to fill in as much as variables either with exact values or regular expressions that you already know into the POJO and then use it to query the database.

There are several different ways we could have done this, depending on how often this data is needed. You could keep the medication information on the **Patient** table, updating it after every encounter which probably is the fastest to access.

#### all encounters for patient id

````
Encounter pe = new Encounter();
// All encounters by patient Id (row key)
List<String> query = Arrays.asList("12345P:.*");
List<Encounter> peL2 = blackBox.fetchByPartialKey(query, pe);
````

#### all encounters for patient with first and last name

````
Encounter pe2 = new Encounter(null, null, firstName, lastName);
List<Encounter> peL4 = blackBox.search(pe2);
````

#### all encounters with specific medication id

````
String diagId = "34532M"; 

Encounter pe = new Encounter();
pe.setMedIds(".*" + diagId + ".*"); // create a regex to match against the string
List<Encounter> peList = blackBox.search(pe);

// if there is no string of medications at the top level, we can also do a deeper search inside 
// the List of medication to find a match, albeit slower
Encounter pe1 = new Encounter(); // an encounter object
Medication d = new Medication(diagId); // the medication object to search for
List<Medication> diags = new ArrayList<>(); 
diags.add(d); // add the medication object to the list of meds
pe1.setDiags(diags); // set that to the encounter

// we now have an object with just the medication we want to search for.
List<Encounter> peList1 = blackBox.search(pe1);
````
#### encounters with a medication for a specific patient by name

````
Encounter pe = new Encounter();
pe.setpFN(firstName);
pe.setpLN(lastname);
pe.setMedIds(".*" + medId + ".*");
List<Encounter> results = blackBox.search(pe);
````

#### all claims for a patient

Again, this data could have been saved in the patient table in order for it to be accessed easily. You could just search the **Claims** table for the particular patient id, if there is a separate table for claims.  The *dirty* way using the current model is to search through all encounters and look for the claim and the patient.

````
Encounter encounter = new Encounter(null, new Patient(null, "Tina", "Warner"));
List<Encounter> encList = blackBox.search(encounter);
for (Encounter enc : encList) {
    System.out.println(enc.getpFN() + " :" + enc.getCls());
}
````

#### all claim charges for a patient

Again, we could iterate over the encounters and get the claim charges. In this example, we will just look through the **Claims** table and add up the amount.

````
Claim cl = new Claim(null, null);
cl.setpId("23456P");

List<Claim> allClaims = blackBox.search(cl);
double totalAmount = 0;
for (Claim thisClaim : allClaims) {
    for (ClaimCharges cCharge : thisClaim.getcCharges()) {
        totalAmount += cCharge.getAmount();
    }
}
````

Depending on the type of queries that you would be executing most often, you can modify the design of POJO further to get faster results. Obviously it won't be just as efficient if you were to do it yourself, as you know the semantics of the variables better. A well thought out row key design for each POJO would be your best bet in making the lookups faster. 

> **TIP**: Do as many fetch(...) calls as you can rather than search(...) calls.

> **TIP**: Another way to look at it is to design your row keys such that you are much more likely to have the complete row keys in most scenarios rather than not.

