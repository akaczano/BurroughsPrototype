import { combineReducers } from 'redux';

import coreReducer from './coreReducer';
import producerReducer from './producerReducer';
import queryReducer from './queryReducer';
import topicReducer from './topicReducer';
import dataReducer from './dataReducer';
import uiReducer from './uiReducer';

export default combineReducers({
    core: coreReducer,
    producer: producerReducer,
    topic: topicReducer,
    query: queryReducer,
    data: dataReducer,
    ui: uiReducer 
});