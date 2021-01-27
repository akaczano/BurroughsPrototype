import {
    SET_CONNECTION,
    SET_TOPICS,
    SET_QUERY_EXECUTING,
    SET_QUERY_EXECUTED,
    SET_QUERY_TERMINATING,
    SET_QUERY_TERMINATED,    
    SET_CODE
} from '../actions/actionTypes';

const initialState = {
    connection: null,
    code: '',
    table: null,
    status: null,
    dbInfo: {
        hostname: null,
        username: null,
        password: null,
        database: null
    },
    topics: [],
    topicSchema: null,
    queryActive: false,
    queryExecuting: false,
    queryTerminating: false
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
            queryActive: true
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
            queryActive: false
        };
    }
    else if (action.type === SET_CODE) {
        return {
            ...state,
            code: action.payload
        };
    }
    return state;
};

export default reducer;