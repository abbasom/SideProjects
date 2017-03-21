import { Component } from '@angular/core';

@Component({
  selector: 'my-app',
  template: `
    <ul>
      <li><a routerLink="/">ASC Home Page</a></li>
       <li><a routerLink="/about">About ASC</a></li>
    </ul>
    <hr />
    <router-outlet></router-outlet>
    `,
})
export class AppComponent  { 
  
}
