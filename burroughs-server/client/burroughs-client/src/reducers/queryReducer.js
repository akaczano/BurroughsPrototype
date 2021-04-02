import {
    SET_QUERY_EXECUTING,
    SET_QUERY_EXECUTED,
    SET_QUERY_TERMINATING,
    SET_QUERY_TERMINATED,    
    SET_STATUS,
    STATUS_RUNNING,
    QUERY_ERROR,
    SET_KEEP_TABLE
} from '../actions/actionTypes';

const initialState = {
    queryActive: false,
    queryExecuting: false,
    queryTerminating: false,
    queryErrorMessage: null,
    statusRunning: false,
    keepTable: false,
    status: null
};

const queryReducer = (state=initialState, action) => {
    if (action.type === SET_QUERY_EXECUTING) {
        return {
            ...state,
            queryExecuting: true
        };
    }
    else if (action.type === SET_QUERY_EXECUTED) {
        return {
            ...state,
            queryExecuting: false,
            queryActive: true,
            queryErrorMessage: null
        }
    }
    else if (action.type === SET_QUERY_TERMINATING) {
        return {
            ...state,
            queryTerminating: true
        };
    }
    else if (action.type === SET_QUERY_TERMINATED) {
        return {
            ...state,
            queryTerminating: false,
            queryActive: false,
            queryErrorMessage: null
        };
    }
    else if (action.type === QUERY_ERROR) {
        return {
            ...state,
            queryExecuting: false,
            queryTerminating: false,
            queryActive: false,
            queryErrorMessage: action.payload
        };
    }
    else if (action.type === SET_KEEP_TABLE) {
        return {
            ...state,
            keepTable: !state.keepTable
        };
    }
    else if (action.type === STATUS_RUNNING) {
        return {
            ...state,
            statusRunning: true
        };
    }
    else if (action.type === SET_STATUS) {                        
        return {
            ...state,
            status: action.payload,
            statusRunning: false,
            queryActive: !action.payload ? false : true
        };
    }
    return state;
};

export default queryReducer;