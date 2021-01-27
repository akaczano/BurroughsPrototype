import React from 'react';
import Editor from 'react-simple-code-editor';
import { highlight, languages} from 'prismjs/components/prism-core';
import 'prismjs/components/prism-sql';
import './prism.css'
import { connect } from 'react-redux';

import { setCode } from '../actions/basicActions';



class CodeEditor extends React.Component {


    render() {
        return (
            <Editor
                value={this.props.code}
                onValueChange={code => this.props.setCode(code)}
                highlight={code => highlight(code, languages.sql)}
                padding={10}
                style={{
                    fontFamily: '"Fira code", "Fira Mono", monospace',
                    fontSize: 12,
                    backgroundColor: 'sepia',
                    height: '100%',
                    border: '0.1px solid black',   
                    margin: '0px',
                    width: '100%',
            
                }}
            />
        );
    }
}

const mapStateToProps = state => {
    return {
        code: state.core.code
    };
};

export default connect(mapStateToProps, { setCode })(CodeEditor);