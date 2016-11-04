var csv = require('csv-parser'),
  fs = require('fs'),
  path = require('path'),
  async = require('async'),
  unzip = require('unzip'),
  Excel = require('exceljs');

var INPUT_DIR = './data/input-zips';
var EXTRACT_DIR = './data/extracted';
var OUTPUT_DIR = './data/output';
var LOOKUP_DIR = './data/lookup';
var PATIENT_HEADERS = ["ExpandedUniquePatientID", "ExpandedUniqueTreatmentPlanID", "Location_OuterPostcode", "GP_PracticeCode", "GP_PracticeName", "GP_Postcode", "% Time In Range", "TargetRange_LowerLimit", "TargetRange_UpperLimit", "Diagnosis", "TreatmentPlanStartDate"];
var INR_HEADERS = ["ExpandedUniqueTreatmentPlanID", "dINRDate", "INR_Value", "pkiTreatmentID", "cStatus"];

var cleanUpExtractedDir = function() {
  var folders = fs.readdirSync(EXTRACT_DIR);
  console.log(folders.length + " directories found in extract dir. Deleting...");
  folders.forEach(function(v) {
    var files = fs.readdirSync(path.join(EXTRACT_DIR, v));
    files.forEach(function(vv) {
      fs.unlinkSync(path.join(EXTRACT_DIR, v, vv));
    });
    fs.rmdirSync(path.join(EXTRACT_DIR, v));
  });
  console.log("Extract dir now empty.");

  var outputFiles = fs.readdirSync(OUTPUT_DIR);
  console.log(outputFiles.length + " files found in output dir. Deleting...");
  outputFiles.forEach(function(v) {
    fs.unlinkSync(path.join(OUTPUT_DIR, v));
  });
  console.log("Output dir now empty.");
};

var loadPostcodeCsvAsync = function(callback) {
  var obj = {};
  fs.createReadStream(path.join(LOOKUP_DIR, 'ccgLookup.csv'))
    .pipe(csv({ separator: ',', headers: ["postcode", "ccg"] }))
    .on('data', function(data) {
      //ignore headers
      obj[data.postcode.replace("  ", " ")] = data.ccg;
    })
    .on('err', function(err) { callback(err); })
    .on('end', function() { callback(null, obj); });
};

var loadCcgCsvAsync = function(callback) {
  var obj = {};
  fs.createReadStream(path.join(LOOKUP_DIR, 'eccg.csv'))
    .pipe(csv({ separator: ',', headers: ["ccg", "name"] }))
    .on('data', function(data) {
      //ignore headers
      obj[data.ccg] = data.name;
    })
    .on('err', function(err) { callback(err); })
    .on('end', function() { callback(null, obj); });
};

var loadPracticeCsvAsync = function(callback) {
  var obj = {};
  fs.createReadStream(path.join(LOOKUP_DIR, 'epraccur.csv'))
    .pipe(csv({ separator: ',', headers: ["practiceId", "postcode", "ccgid", "ccgname"] }))
    .on('data', function(data) {
      //ignore headers
      obj[data.practiceId] = data.ccgname;
    })
    .on('err', function(err) { callback(err); })
    .on('end', function() { callback(null, obj); });
};

var readPatientCsvAsync = function(filepath, callback) {
  var obj = { plans: {}, patients: {} };
  fs.createReadStream(filepath)
    .pipe(csv({ separator: '\t', headers: PATIENT_HEADERS }))
    .on('data', function(data) {
      //ignore headers
      if (data[PATIENT_HEADERS[0]] === PATIENT_HEADERS[0]) return;

      if (obj.plans[data[PATIENT_HEADERS[1]]]) {
        console.log("Already got an ExpandedUniqueTreatmentPlanID for: " + obj.plans[data[PATIENT_HEADERS[1]]]);
        throw new Error("Uh oh");
      }
      if (!obj.patients[data[PATIENT_HEADERS[0]]]) {
        obj.patients[data[PATIENT_HEADERS[0]]] = [];
      }
      obj.patients[data[PATIENT_HEADERS[0]]].push(data);
      obj.plans[data[PATIENT_HEADERS[1]]] = data;
      diagnoses[data.Diagnosis] = 1;
    })
    .on('err', function(err) { callback(err); })
    .on('end', function() { callback(null, obj); });
};

var readInrCsvAsync = function(filepath, UniqueTreatmentPlanIDs, callback) {
  var obj = {},
    maxDate = new Date(1900, 1, 1);
  fs.createReadStream(filepath)
    .pipe(csv({ separator: '\t', headers: INR_HEADERS }))
    .on('data', function(data) {
      if (data[INR_HEADERS[0]] === INR_HEADERS[0]) return;

      if (UniqueTreatmentPlanIDs.indexOf(data[INR_HEADERS[0]]) === -1) {
        console.log("Treatment plan id: " + data[INR_HEADERS[0]] + " does not exist in the patient file");
        throw new Error("Uh oh");
      }

      if (!obj[data[INR_HEADERS[0]]]) {
        obj[data[INR_HEADERS[0]]] = [];
      }
      var dt = new Date(data.dINRDate.split('/')[2], +data.dINRDate.split('/')[1] - 1, data.dINRDate.split('/')[0]);
      if (dt > maxDate) maxDate = dt;
      obj[data[INR_HEADERS[0]]].push({ date: dt, inr: data.INR_Value });
    })
    .on('err', function(err) { callback(err); })
    .on('end', function() { callback(null, obj, maxDate); });
};

var processTreatmentPlan = function(planList, plan, patient, ccg, maxDate) {

  var rtn = { two5orone8: false, two1point5: false, ttr: 0, ttrlt65: false };
  if (planList.filter(function(v) {
      return (+v.inr > 5) && (maxDate - v.date) < (365 * 24 * 60 * 60 * 1000 / 2);
    }).length >= 2 || planList.filter(function(v) {
      return (+v.inr > 8) && (maxDate - v.date) < (365 * 24 * 60 * 60 * 1000 / 2);
    }).length >= 1) {
    rtn.two5orone8 = true;
  }

  if (planList.filter(function(v) {
      return (+v.inr < 1.5) && (maxDate - v.date) < (365 * 24 * 60 * 60 * 1000 / 2);
    }).length >= 1) {
    rtn.two1point5 = true;
  }

  rtn.ttr = +plan["% Time In Range"];
  rtn.ttrlt65 = rtn.ttr < 65;

  if (patient.ccg && patient.ccg != ccg) {
    console.log("Patient appears in two ccgs: " + patient.ExpandedUniquePatientID);
  }

  patient.ccg = ccg;
  patient.two5orone8 = patient.two5orone8 || rtn.two5orone8;
  patient.two1point5 = patient.two1point5 || rtn.two1point5;
  patient.ttr = rtn.ttr;
  patient.ttrlt65 = patient.ttrlt65 || rtn.ttrlt65;

};

cleanUpExtractedDir();

var output = {},
  patientFullList = [],
  diagnoses = {};

loadPostcodeCsvAsync(function(err, postcodeLookup) {
  console.log("Postcode lookup loaded.");
  console.log(Object.keys(postcodeLookup).length + " practices found.");
  loadCcgCsvAsync(function(err, ccgLookup) {
    console.log("CCG lookup loaded.");
    console.log(Object.keys(ccgLookup).length + " found.");
    loadPracticeCsvAsync(function(err, practiceLookup) {
      console.log("Practice lookup loaded.");
      console.log(Object.keys(practiceLookup).length + " practices found.");

      //Get all zip files
      fs.readdir(INPUT_DIR, function(err, files) {
        var items = files.length,
          done = 0;
        files.forEach(function(file) {
          var stream = fs.createReadStream(path.join(INPUT_DIR, file));
          var unzipStream = unzip.Extract({
            path: path.join(EXTRACT_DIR, file)
          });
          stream.pipe(unzipStream);
          var had_error = false;
          unzipStream.on('error', function(err) {
            had_error = true;
            console.log(err);
          });
          unzipStream.on('close', function() {
            if (!had_error) {
              //console.log(file + " closed");
            }
            //get files in directory
            var zipFiles = fs.readdirSync(path.join(EXTRACT_DIR, file));
            var inrFile, patientFile;
            if (zipFiles[0].search(/inr/i) > -1) inrFile = zipFiles[0];
            else if (zipFiles[0].search(/pat/i) > -1) patientFile = zipFiles[0];
            else console.log(zipFiles[0] + " in " + file + " not recognised as inr or patient");

            if (zipFiles[1].search(/inr/i) > -1) inrFile = zipFiles[1];
            else if (zipFiles[1].search(/pat/i) > -1) patientFile = zipFiles[1];
            else console.log(zipFiles[1] + " in " + file + " not recognised as inr or patient");

            //if (inrFile) console.log("INR file: " + path.join(EXTRACT_DIR, file, inrFile));
            //if (patientFile) console.log("PAT file: " + path.join(EXTRACT_DIR, file, patientFile));

            readPatientCsvAsync(path.join(EXTRACT_DIR, file, patientFile), function(err, patients) {
              readInrCsvAsync(path.join(EXTRACT_DIR, file, inrFile), Object.keys(patients.plans), function(err, results, maxDate) {
                //console.log(maxDate);
                var ccg = "";
                var ccgs = {};
                var log = {
                  "PracticeCodeNotFoundPostCodeNotFound": 0,
                  "PracticeCodeNotFoundNoPostcode": 0,
                  "NoPracticeCodePostCodeNotFound": 0,
                  "NoPracticeCodeNoPost": 0
                };
                Object.keys(results).forEach(function(v) {
                  if (patients.plans[v]) {
                    if (patients.plans[v].GP_PracticeCode) {
                      if (!practiceLookup[patients.plans[v].GP_PracticeCode]) {
                        if (patients.plans[v].GP_Postcode) {
                          if (postcodeLookup[patients.plans[v].GP_Postcode] && ccgLookup[postcodeLookup[patients.plans[v].GP_Postcode]]) {
                            if (ccgLookup[postcodeLookup[patients.plans[v].GP_Postcode]] != ccg) {
                              ccg = ccgLookup[postcodeLookup[patients.plans[v].GP_Postcode]];
                              ccgs[ccg] = 1;
                            } else {
                              // do nothing
                            }
                          } else {
                            //console.log("Practice code not found and postcode not found for " + patients.plans[v].ExpandedUniquePatientID);
                            log.PracticeCodeNotFoundPostCodeNotFound++;
                            return;
                          }
                        } else {
                          //console.log("Practice code not found and no postcode for " + patients.plans[v].ExpandedUniquePatientID);
                          log.PracticeCodeNotFoundNoPostcode++;
                          return;
                        }
                      } else if (practiceLookup[patients.plans[v].GP_PracticeCode] !== ccg) {
                        ccg = practiceLookup[patients.plans[v].GP_PracticeCode];
                        ccgs[ccg] = 1;
                      } else {
                        // do nothing
                      }
                    } else {
                      if (patients.plans[v].GP_Postcode) {
                        if (postcodeLookup[patients.plans[v].GP_Postcode] && ccgLookup[postcodeLookup[patients.plans[v].GP_Postcode]]) {
                          if (ccgLookup[postcodeLookup[patients.plans[v].GP_Postcode]] != ccg) {
                            ccg = ccgLookup[postcodeLookup[patients.plans[v].GP_Postcode]];
                            ccgs[ccg] = 1;
                          } else {
                            // do nothing
                          }
                        } else {
                          //console.log("No practice code and postcode not found for " + patients.plans[v].ExpandedUniquePatientID);
                          log.NoPracticeCodePostCodeNotFound++;
                          return;
                        }
                      } else {
                        //console.log("No postcode or practice code for " + patients.plans[v].ExpandedUniquePatientID);
                        log.NoPracticeCodeNoPost++;
                        return;
                      }
                    }
                    if (!ccg) {
                      var x1 = patients.plans[v].GP_PracticeCode;
                      var x2 = practiceLookup[patients.plans[v].GP_PracticeCode];
                      var x3 = patients.plans[v].GP_Postcode;
                      var x4 = postcodeLookup[patients.plans[v].GP_Postcode];
                      var x5 = ccgLookup[postcodeLookup[patients.plans[v].GP_Postcode]];
                      console.log("NO CCG:" + patients.plans[v].ExpandedUniquePatientID);
                    }
                    processTreatmentPlan(results[v], patients.plans[v], patients.patients[patients.plans[v].ExpandedUniquePatientID], ccg, maxDate);
                  } else {
                    console.log("oops - a plan with no patient??" + v);
                  }
                });

                console.log(file + " has " + JSON.stringify(log));

                var outputObject = {};
                Object.keys(ccgs).forEach(function(v) {
                  outputObject[v] = {
                    counttwo1point5: 0,
                    counttwo5orone8: 0,
                    countttrlt65: 0,
                    countall: 0,
                    countwarfarin: 0
                  };
                });

                var patientList = Object.keys(patients.patients).forEach(function(v) {
                  if (!patients.patients[v].ttr && patients.patients[v].ttr !== 0) {
                    if (patients.patients[v].ccg) {
                      console.log("might have some data for: " + v);
                      return;
                    } else {
                      //no ccg
                      //could get it from patient postcode
                      //console.log("no data for patient: " + v);
                      return;
                    }
                  } else {
                    patientFullList.push([v, patients.patients[v].ccg, patients.patients[v].two1point5, patients.patients[v].two5orone8, patients.patients[v].ttrlt65]);
                    if (patients.patients[v].two1point5) outputObject[patients.patients[v].ccg].counttwo1point5++;
                    if (patients.patients[v].two5orone8) outputObject[patients.patients[v].ccg].counttwo5orone8++;
                    if (patients.patients[v].ttrlt65) outputObject[patients.patients[v].ccg].countttrlt65++;
                    if (patients.patients[v].two1point5 || patients.patients[v].two5orone8 || patients.patients[v].ttrlt65) outputObject[patients.patients[v].ccg].countall++;
                    outputObject[patients.patients[v].ccg].countwarfarin++;
                  }
                });
                Object.keys(outputObject).forEach(function(v) {
                  if (!output[v]) {
                    output[v] = {
                      counttwo1point5: 0,
                      counttwo5orone8: 0,
                      countttrlt65: 0,
                      countall: 0,
                      countwarfarin: 0
                    };
                  }
                  output[v].counttwo1point5 += outputObject[v].counttwo1point5;
                  output[v].counttwo5orone8 += outputObject[v].counttwo5orone8;
                  output[v].countttrlt65 += outputObject[v].countttrlt65;
                  output[v].countall += outputObject[v].countall;
                  output[v].countwarfarin += outputObject[v].countwarfarin;
                });
                done++;
                if (done === items) {
                  var outputArray = [
                    [
                      "CCG",
                      "Patients-with-2-INR->5-or-1-INR-<8-in-last-6 months",
                      "Patients-with-2-INR-<1.5-in-last-6-months",
                      "Patients-with-TTR-<65%",
                      "Number-of-unique-patients-breaching-any-target",
                      "Warfarin-Population"
                    ]
                  ];
                  outputArray = outputArray.concat(Object.keys(output).map(function(v) {
                    return [v, output[v].counttwo1point5, output[v].counttwo5orone8, output[v].countttrlt65, output[v].countall, output[v].countwarfarin];
                  }));

                  var workbook = new Excel.Workbook();
                  var now = new Date();
                  workbook.creator = 'Richard Williams';
                  workbook.lastModifiedBy = 'Richard Williams';
                  workbook.created = now;
                  workbook.modified = now;
                  var sheet1 = workbook.addWorksheet('Results');
                  outputArray.forEach(function(row, i) {
                    row.forEach(function(cell, j) {
                      sheet1.getRow(i + 1).getCell(j + 1).value = cell;
                    });
                  });

                  workbook.xlsx.writeFile(path.join(OUTPUT_DIR, 'results.xlsx'))
                    .then(function() {
                      // done
                    });

                  var workbook2 = new Excel.Workbook();
                  workbook2.creator = 'Richard Williams';
                  workbook2.lastModifiedBy = 'Richard Williams';
                  workbook2.created = now;
                  workbook2.modified = now;
                  var sheet2 = workbook2.addWorksheet('Results');
                  patientFullList.forEach(function(row, i) {
                    row.forEach(function(cell, j) {
                      sheet2.getRow(i + 1).getCell(j + 1).value = cell;
                    });
                  });

                  workbook2.xlsx.writeFile(path.join(OUTPUT_DIR, 'full_results.xlsx'))
                    .then(function() {
                      // done
                    });

                  var workbook3 = new Excel.Workbook();
                  var sheet3 = workbook3.addWorksheet('Diagnoses');
                  sheet3.getRow(1).getCell(1).value = "Diagnosis";
                  Object.keys(diagnoses).sort().forEach(function(v, i) {
                    sheet3.getRow(i + 2).getCell(1).value = v;
                  });

                  workbook3.xlsx.writeFile(path.join(OUTPUT_DIR, 'diagnoses.xlsx'))
                    .then(function() {
                      // done
                    });
                }
              });
            });

          });
        });
      });
    });
  });
});
