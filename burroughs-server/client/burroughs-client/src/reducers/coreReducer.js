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
    SET_KEEP_TABLE,
    APPEND_MESSAGE,
    SET_DATA,
    CLOSE_DESCRIPTION,
    SET_SCHEMA,
    DATA_LOADING,
    DATA_ERROR,
    TOPIC_DELETING,
    TOPIC_DELETED,
    DELETE_ERROR
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
    topicDeleting: false,
    topicDeleteError: null,
    queryActive: false,
    queryExecuting: false,
    queryTerminating: false,
    queryErrorMessage: null,
    statusRunning: false,
    keepTable: false,
    consoleMessages: [],
    lastConsoleRequest: Date.now(),
    data: null,
    dataLoading: false,
    dataError: null    
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
                username: action.payload.username,
                table: action.payload.table,
                data: null
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
            statusRunning: false,
            queryActive: !action.payload ? false : true
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
    else if (action.type === APPEND_MESSAGE) {
        let newMessages = state.consoleMessages.slice();
        for (const m of action.payload) {
            if (!newMessages.find(mes => mes.time === m.time) && m.text.length > 0) {
                newMessages.push(m);
            }
        }
        return {
            ...state,
            consoleMessages: newMessages,
            lastConsoleRequest: Date.now()
        };
    }
    else if (action.type === SET_DATA) {
        return {
            ...state,
            data: action.payload,
            dataLoading: false,
            dataError: null
        };
    }
    else if (action.type === DATA_LOADING) {
        return {
            ...state,
            dataLoading: true
        };
    }
    else if (action.type === DATA_ERROR) {        
        return {
            ...state,
            dataLoading: false,
            dataError: action.payload
        };
    }
    else if (action.type === SET_SCHEMA) {        
        return {
            ...state,
            topicSchema: action.payload,
            topicDeleteError: null
        };
    }
    else if (action.type === CLOSE_DESCRIPTION) {
        return {
            ...state,
            topicSchema: null
        };
    }    
    else if (action.type === TOPIC_DELETING) {
        return {
            ...state,
            topicDeleting: true
        };
    }
    else if (action.type === TOPIC_DELETED) {
        return {
            ...state,
            topicDeleting: false,
            topicSchema: null
        };
    }
    else if (action.type === DELETE_ERROR) {
        return {
            ...state,
            topicDeleting: false,
            topicDeleteError: action.payload
        };
    }
    return state;
};

export default reducer;