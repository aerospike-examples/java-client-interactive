import { AfterViewInit, Component, ElementRef, EventEmitter, HostListener, OnInit, ViewChild } from '@angular/core';
import { TimingData } from '../../model/timing-data.model';
import { TimingPoint } from '../../model/timing-point.model';
import { WorkloadDesc } from '../../model/workload-desc.model';
import { AerospikeDataSourceService } from '../../services/aerospike-data-source.service';
import { ParamValue } from '../../model/param-value.model';
import { InvocationResult } from '../../model/invocation-result.model';
import { WorkloadStatus } from '../../model/workload-status.model';
import { MenuItem } from 'primeng/api';
import { WorkloadResult } from '../../model/workload-result.model';
import { SystemPreferences } from '../../model/system-preferences.model';
import { ActivatedRoute, Router } from '@angular/router';

@Component({
  selector: 'app-simulation',
  templateUrl: './app-simulation.component.html',
  styleUrls: ['./app-simulation.component.css']
})

export class AppSimulationComponent implements AfterViewInit, OnInit {
  @ViewChild("accordion", {static:false}) 
  accordion!: ElementRef;

  items!: MenuItem[];
  networkConfigItems! : MenuItem[];

  status = '';

  AGGREGATION_WORKLOAD = 'Aggregation Counter';
  showDialog = false;
  title = 'workload-simulation-ui-src';
  currentData : any = {'Aggregation Counter' : { results:[] }};
  activeWorkloads : string[] = [];
  startTime = 0;
  MAX_READINGS = 3600;
  // WORKLOAD1 = "WORKLOAD1";
  // WORKLOAD2 = "WORKLOAD2";
  LATENCY = "LATENCY";
  THROUGHPUT = "THROUGHPUT";

  systemPreferences : SystemPreferences = {doLogging: false, loggingDir : '/tmp', workloadName: '', graphRefreshMs: 350, networkRefreshMs: 1000};
  editingSystemPreferences : SystemPreferences = {...this.systemPreferences};

  workloadValues : any = null;
  valuesComputed = false;
  selected : boolean[] = [];

  options =['first', 'second', 'third'];
  test : any;

  activeLoading : boolean = false;
  workloadResults : WorkloadResult[] = [];

  commsErrorDialog : boolean = false;
  commsErrorCount : number = 0;
  timer : any;

  private minDuration = 60*1000;
  private maxDuration = this.MAX_READINGS * 1000;
  duration = 3 * 60 * 1000;

  ybOptions = ['Yugabyte DB', 'Yugabyte Anywhere', 'Yugabyte Managed'];
  ybOption = this.ybOptions[0];
  
  showConfigDialog = false;
  configStatus = "";
  existingUser = false;
  passwordValidated = false;
  password = "";
  confirmPassword = "";

  constructor(private dataSource : AerospikeDataSourceService,
            private router: Router ) {
    this.timer = setInterval(() => {
      this.getResults();
    },350);

    this.getSystemPreferences();
  }

  ngOnInit() {
    this.networkConfigItems = [
      {
        label: 'Options',
        items: [{
          label: 'Aerospike Options',
          icon: 'pi pi-cog',
          command: (evt) => {
              this.showAerospikeOptions();
          }
        }]
      }
    ];

  }

  ngAfterViewInit() {
  }

  showAerospikeOptions() {
    //this.router.navigateByUrl("/configuration");
    this.showConfigDialog = true;
  }

  cancelConfigDialog() {
    this.showConfigDialog = false;
  }

  closeConfigDialog() {
  }

  private extractWorkloadIdFromEvent(evt : any)  : string {
    let control = evt.originalEvent.srcElement.closest('.workload-inst');
    let classes = control.classList;
    for (const thisClass of classes) {
      if (thisClass.match(/^[A-Z_]+_\d+$/)) {
        return thisClass;
      }
    }
    return '';
  }

  private setSystemPreferences(preferences : SystemPreferences) {
    this.systemPreferences = preferences;
    clearInterval(this.timer);
    this.timer = setInterval(() => {
      this.getResults();
    }, preferences.graphRefreshMs);
  }

  getSystemPreferences() {
    this.dataSource.getSystemPreferences().subscribe(result => {
      this.setSystemPreferences(result);
    })
  }
  saveSystemSettings() {
    this.dataSource.saveSystemPreferences(this.editingSystemPreferences).subscribe(result => {
      this.status = "System Preferences Saved";
      this.setSystemPreferences(this.editingSystemPreferences);
    });
  }

  computeWorkloadValues(workloads : WorkloadDesc[]) {
    // let workloads = this.workloadService.getWorkloads();
    this.workloadValues = {};
    for (let i = 0; i < workloads.length; i++) {
      let thisWorkload = workloads[i];
      let currentValues : any = {};
      for (let j = 0; j < thisWorkload.params.length; j++) {
        let thisParam = thisWorkload.params[j];
        switch (thisParam.type) {
          case 'NUMBER':
            if (thisParam.defaultValue) {
              currentValues[thisParam.name] = thisParam.defaultValue.intValue || 0;
            }
            else {
              currentValues[thisParam.name] = 0;
            }
            break;

          case 'BOOLEAN':
            if (thisParam.defaultValue) {
              currentValues[thisParam.name] = thisParam.defaultValue.boolValue || false;
            }
            else {
              currentValues[thisParam.name] = false;
            }
            break;

          case 'STRING':
            if (thisParam.defaultValue) {
              currentValues[thisParam.name] = thisParam.defaultValue.stringValue || false;
            }
            else {
              currentValues[thisParam.name] = '';
            }
            break;
  
        }
      }
      this.workloadValues[thisWorkload.workloadId] = currentValues;
    }
    this.valuesComputed = true;
  }

  private refreshActiveTasks() {
    //this.activeLoading = true;
    this.dataSource.getActiveWorkloads().subscribe(workloads => {
      this.activeLoading = false;
      this.workloadResults = workloads;
    });
  }

  timerId : any = null;
  // Called when the tab changes.
  handleChange(e : any) {
    if (this.timerId) {
      clearInterval(this.timerId);
      this.timerId = null;
    }
    if (e.index == 1) {
      this.refreshActiveTasks();
      this.timerId = setInterval(() => this.refreshActiveTasks(), 2000);
    }
  }

  getResults() {
    this.dataSource.getTimingResults(this.startTime).subscribe(data => {
      this.commsErrorCount = 0;
      let lastCommsErrorDialog = this.commsErrorDialog;
      this.commsErrorDialog = false;

      // Iterate through the workloads based on the information here. 
      if (!data[this.AGGREGATION_WORKLOAD]) {
        return;
      }
      this.activeWorkloads = [];

      let newData : any = {};

      for (const metricName in data) {
        if (data[metricName].canBeTerminated) {
          this.activeWorkloads.push(metricName);
        }
        // let metricName = this.AGGREGATION_WORKLOAD;

        newData[metricName] = data[metricName];

        if (this.startTime == 0 || !this.currentData[metricName]) {
          this.currentData[metricName] = newData[metricName];
        }
        else {
          let currentResults = this.currentData[metricName].results;
          this.currentData[metricName] = {...newData[metricName]};
          this.currentData[metricName].results = currentResults;
          // Append these results to the existing data and trim the front if needed
          this.currentData[metricName].results = this.currentData[metricName].results.concat(newData[metricName].results);
        }
        if (this.currentData[metricName].results.length > this.MAX_READINGS) {
          this.currentData[metricName].results.splice(0, this.currentData[metricName].results.length-this.MAX_READINGS);
        }
        this.currentData[metricName].results = [].concat(this.currentData[metricName].results);
      }
      let aggregateResults = this.currentData[this.AGGREGATION_WORKLOAD].results;
      if (aggregateResults.length > 0) {
        this.startTime = aggregateResults[aggregateResults.length-1].startTimeMs;
      }
    },
    (error) => {
      if (!this.commsErrorDialog) {
        if (++this.commsErrorCount > 5) {
          this.commsErrorDialog = true;
        }
      }
    });
  }
  @HostListener('wheel', ['$event'])
  onMouseWheel(event : any) {
    // if ((event.srcElement.closest('p-dialog') == null)  && (event.srcElement.closest('figure') != null) || event.shiftKey) {
   if (event.shiftKey) {
      event.preventDefault();
      let amount = event.wheelDelta;
      let change = 1+(amount/1200);
      this.duration = Math.floor(Math.max(this.minDuration, Math.min(this.maxDuration, this.duration * change)));
    }
  } 

  displayDialog() {
    // setTimeout(() => {
    //   const accordionElement = this.accordion.nativeElement;
    //   if (accordionElement) {
    //     let workloadElementList = accordionElement.querySelectorAll('[role="region"]');
    //     for (let i = 0; i < workloadElementList.length; i++) {
    //       let workloadElement = workloadElementList[i];
    //       (workloadElement as any).setAttribute('style', 'height:0; overflow:hidden;');
    //     }
    //   }
    // },100);

    this.editingSystemPreferences = {...this.systemPreferences};
    this.status = "";
    this.showDialog = true;
    // this.computeWorkloadValues(this.workloadService.getWorkloads());
  }

  closeDialog() {
    this.showDialog = false;
  }
}
