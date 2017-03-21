import {Injectable} from '@angular/core';
import {Http} from '@angular/http';
import 'rxjs/add/operator/map';

@Injectable()
export class PostsService {
    constructor(private http: Http){
        console.log('PostsService Initialized...');
    }

    getDataSources(){
        return this.http.get('http://34.194.42.167:8090/asc_rest/rest/zeppelin/query/2CD83E4Q5')
            .map(res => res.json());
    }
}