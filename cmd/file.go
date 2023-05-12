package cmd

import (
	"github.com/viant/afs/url"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/toolbox/format"
	"path"
	"strings"
)

type (
	session struct {
		sourceURL   string
		output      outputFile
		routeConfig *ViewConfig
		basePath    string
		ruleName    string
		pathDiff    string
		routePrefix string
	}

	outputFile struct {
		goFileURL string
		sqlURL    string
		pluginURL string
		sampleURL string
	}
)

func newSession(basePath string, source string, pluginPath string, sampleFilePath string, templatesDst string, goFileOutput string) *session {
	if sampleFilePath == "" {
		sampleFilePath = basePath
	}

	if !strings.HasPrefix(goFileOutput, "/") {
		goFileOutput = url.Join(basePath, goFileOutput)
	}

	return &session{
		basePath:  basePath,
		sourceURL: source,
		output: outputFile{
			sqlURL:    templatesDst,
			goFileURL: goFileOutput,
			pluginURL: pluginPath,
			sampleURL: sampleFilePath,
		},
	}
}

func (f *session) TemplateURL(fileName string) string {
	if path.IsAbs(f.output.sqlURL) {
		return url.Join(f.output.sqlURL, fileName)
	}

	return url.Join(f.basePath, f.output.sqlURL, fileName)
}

func (f *session) GoFileURL(fileName string) string {
	if ext := path.Ext(f.output.goFileURL); ext == ".go" {
		if path.IsAbs(f.output.goFileURL) {
			return f.output.goFileURL
		}

		return url.Join(f.basePath, f.output.goFileURL)
	}

	detectCase, err := format.NewCase(formatter.DetectCase(fileName))
	if err == nil {
		fileName = detectCase.Format(fileName, format.CaseLowerUnderscore)
	}

	return url.Join(f.output.goFileURL, fileName)
}

func (f *session) PluginURL(fileName string) string {
	return url.Join(f.output.pluginURL, fileName)
}

func (f *session) SampleFileURL(fileName string) string {
	return url.Join(f.output.sampleURL, fileName)
}

func (f *session) setMainViewConfig(config *ViewConfig) {
	f.routeConfig = config
	if f.output.sqlURL == "" {
		mainViewFileName := f.routeConfig.viewName
		if ext := path.Ext(mainViewFileName); ext != "" {
			mainViewFileName = strings.Replace(mainViewFileName, ext, "", 1)
		}

		f.output.sqlURL = mainViewFileName
		f.ruleName = mainViewFileName
	}
}

func (f *session) RelativeOfBasePath(destURL string) string {
	aPath := strings.Replace(destURL, f.basePath, "", 1)
	aPath = strings.TrimLeft(aPath, "/")
	if f.pathDiff != "" {
		aPath = path.Join(f.pathDiff, aPath)
	}

	return aPath
}

func (f *session) JoinWithSourceURL(aPath string) string {
	if strings.HasPrefix(aPath, "/") {
		return aPath
	}

	return url.JoinUNC(url.Dir(f.sourceURL), aPath)
}
