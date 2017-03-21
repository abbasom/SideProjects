import { Component } from '@angular/core';
import {PostsService} from '../services/posts.service';

@Component({
    moduleId: module.id,
    selector: 'user',
    templateUrl: 'user.component.html',
    providers: [PostsService]
})
export class UserComponent  { 
  name: string; 
  email: string;
  address: address;
  rules: string[];
  showRules: boolean;
  showData: boolean;
  dataSources:DataSource[];

  constructor(private postsService: PostsService){
    this.name = ''; 
    this.email = '',
    this.rules = [''];
    this.showRules = false;
    this.postsService.getDataSources().subscribe(dataSources => { 
         this.dataSources = dataSources; 
         this.showData = false;
     }); 
    }

  toggleRules(){
      if(this.showRules == true){
          this.showRules = false;
      } else {
        this.showRules = true;
      }
  }
  toggleDataSources(){
      if(this.showData == true){
          this.showData = false;
      } else {
        this.showData = true;
      }
  }

  addRule(rule:any){
      this.rules.push(rule);
  }

  deleteRule(i:any){
      this.rules.splice(i, 1);
  }
}

interface address {
    street: string;
    city: string;
    state: string;
}

interface DataSource{
    id: string;
    name: string;
    description: string;
    schema: string;
    source: Object;
    dataTypes: string[];
    classification: string;
    creationDate: string;
    maturity: string;
}



