import {
    SET_CONNECTION,
    SET_TOPICS,
    SET_QUERY_EXECUTING,
    SET_QUERY_EXECUTED,
    SET_QUERY_TERMINATING,
    SET_QUERY_TERMINATED,    
    SET_CODE,
    SET_OUTPUT_TABLE,
    QUERY_ERROR,
    SET_DATABASE,
    SET_STATUS,
    STATUS_RUNNING,
    SET_KEEP_TABLE
} from '../actions/actionTypes';

const initialState = {
    connection: null,
    code: '',
    table: null,
    status: null,
    dbInfo: {
        hostname: '',
        username: '',
        password: '',
        database: '',
        table: ''
    },
    topics: [],
    topicSchema: null,
    queryActive: false,
    queryExecuting: false,
    queryTerminating: false,
    queryErrorMessage: null,
    statusRunning: false,
    keepTable: false
};

const reducer = (state = initialState, action) => {
    if (action.type === SET_CONNECTION) {
        return {
            ...state,
            connection: action.payload
        }
    }
    else if (action.type === SET_TOPICS) {
        return {
            ...state,
            topics: action.payload
        };
    }
    else if (action.type === SET_QUERY_EXECUTING) {
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
    else if (action.type === SET_DATABASE) {        
        return {
            ...state,
            dbInfo: {
                ...state.dbInfo,
                database: action.payload.database,
                hostname: action.payload.hostname,
                username: action.payload.username
            }
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
            statusRunning: false
        };
    }
    else if (action.type === SET_CODE) {
        return {
            ...state,
            code: action.payload
        };
    }
    else if (action.type === SET_OUTPUT_TABLE) {
        return {
            ...state,
            dbInfo: {
                ...state.dbInfo,
                table: action.payload
            }
        };
    }
    else if (action.type === SET_KEEP_TABLE) {
        return {
            ...state,
            keepTable: !state.keepTable
        };
    }
    return state;
};

export default reducer;