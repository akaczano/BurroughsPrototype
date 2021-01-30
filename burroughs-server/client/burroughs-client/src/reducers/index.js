import { combineReducers } from 'redux';

import coreReducer from './coreReducer';
import producerReducer from './producerReducer';

export default combineReducers({
    core: coreReducer,
    producer: producerReducer 
});