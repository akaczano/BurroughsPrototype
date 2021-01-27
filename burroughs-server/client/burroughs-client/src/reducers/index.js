import { combineReducers } from 'redux';

import coreReducer from './coreReducer';

export default combineReducers({
    core: coreReducer 
});